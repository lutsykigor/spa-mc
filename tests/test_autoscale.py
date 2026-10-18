from spa_mc.autoscale import Autoscaler, Decision, WorkerSample


def samples(*ratios: float, parked: tuple[int, ...] = ()) -> list[WorkerSample]:
    return [WorkerSample(i, i not in parked, ratio) for i, ratio in enumerate(ratios)]


def test_quiet_workers_need_no_change():
    assert Autoscaler(park_on_steal=True).decide(samples(0.0, 0.01, 0.02, 0.0), budget=4) == Decision()


def test_a_worker_is_parked_only_after_sustained_steal():
    scaler = Autoscaler(park_on_steal=True)
    assert scaler.decide(samples(0.6, 0.0, 0.0), budget=3) == Decision()
    assert scaler.decide(samples(0.6, 0.0, 0.0), budget=3) == Decision(park=frozenset({0}))


def test_a_single_spike_does_not_park():
    scaler = Autoscaler(park_on_steal=True)
    scaler.decide(samples(0.6, 0.0), budget=2)
    assert scaler.decide(samples(0.0, 0.0), budget=2) == Decision()
    assert scaler.decide(samples(0.6, 0.0), budget=2) == Decision()


def test_the_most_stolen_worker_is_parked_first_one_at_a_time():
    scaler = Autoscaler(park_on_steal=True)
    scaler.decide(samples(0.5, 0.8, 0.4), budget=3)
    assert scaler.decide(samples(0.5, 0.8, 0.4), budget=3) == Decision(park=frozenset({1}))


def test_the_last_active_worker_is_never_parked():
    scaler = Autoscaler(park_on_steal=True)
    for _ in range(5):
        decision = scaler.decide(samples(0.9, 0.0, parked=(1,)), budget=2)
        assert 0 not in decision.park


def test_a_parked_worker_returns_after_its_core_stays_quiet():
    scaler = Autoscaler(park_on_steal=True)
    quiet = samples(0.0, 0.0, parked=(0,))
    assert scaler.decide(quiet, budget=2) == Decision()
    assert scaler.decide(quiet, budget=2) == Decision()
    assert scaler.decide(quiet, budget=2) == Decision(unpark=frozenset({0}))


def test_steal_parking_is_off_by_default():
    scaler = Autoscaler()
    for _ in range(5):
        assert scaler.decide(samples(0.9, 0.0), budget=2) == Decision()


def test_a_shrinking_budget_parks_the_excess_immediately():
    decision = Autoscaler(park_on_steal=True).decide(samples(0.0, 0.2, 0.0, 0.1), budget=2)
    assert decision == Decision(park=frozenset({1, 3}))


def test_a_growing_budget_spawns_and_unparks_within_it():
    scaler = Autoscaler(park_on_steal=True)
    quiet = samples(0.0, 0.0, 0.0, parked=(1, 2))
    for _ in range(2):
        scaler.decide(quiet, budget=5)
    assert scaler.decide(quiet, budget=5) == Decision(unpark=frozenset({1, 2}), spawn=2)


def test_parked_workers_stay_parked_while_the_budget_has_no_room():
    scaler = Autoscaler(park_on_steal=True)
    quiet = samples(0.0, 0.0, 0.0, parked=(2,))
    for _ in range(5):
        assert scaler.decide(quiet, budget=2) == Decision()
