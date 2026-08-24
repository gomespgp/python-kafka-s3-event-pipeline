def test_start_simulation_success(client, mock_event_generator):
    mock_event_generator.start.return_value = True
    response = client.post("/api/v1/simulation/start?interval_seconds=4")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "started"
    assert data["interval_seconds"] == 4
    mock_event_generator.start.assert_called_once_with(interval_seconds=4)


def test_start_simulation_already_running(client, mock_event_generator):
    mock_event_generator.start.return_value = False
    response = client.post("/api/v1/simulation/start")
    assert response.status_code == 200
    assert response.json()["status"] == "already_running"


def test_start_simulation_invalid_interval(client, mock_event_generator):
    response = client.post("/api/v1/simulation/start?interval_seconds=0")
    assert response.status_code == 422  # validation fails (ge=1)


def test_stop_simulation(client, mock_event_generator):
    response = client.post("/api/v1/simulation/stop")
    assert response.status_code == 200
    assert response.json()["status"] == "stopped"
    mock_event_generator.stop.assert_called_once()


def test_get_simulation_status(client, mock_event_generator):
    mock_event_generator.is_running = True
    response = client.get("/api/v1/simulation/status")
    assert response.status_code == 200
    assert response.json()["is_running"] is True
