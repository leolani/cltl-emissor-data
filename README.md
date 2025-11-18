# cltl-emissor-data

Service for storing and retrieving conversational data in the EMISSOR format. This component provides persistent storage for scenarios, signals (text, audio, image), mentions, and annotations, along with HTTP endpoints for data access and download.

## Description

The EMISSOR data service collects interaction data from the event bus and stores it in structured JSON files following the EMISSOR data model. Each conversation (scenario) is stored in a separate directory with all associated signals, mentions, and annotations. The service also provides HTTP endpoints for browsing and downloading archived scenarios.

## Features

- **Event-driven data collection**: Automatically captures signals and mentions from the event bus
- **Structured storage**: Organizes data by scenario with JSON serialization
- **Multi-modal support**: Handles text, audio, and image signals
- **HTTP API**: REST endpoints for listing, retrieving, and downloading scenarios
- **RDF integration**: Automatically copies relevant brain logs on scenario completion
- **Configurable storage**: File-based storage with configurable location

## Installation

### From PyPI

```bash
pip install cltl.emissor-data
```

### From source

```bash
git clone https://github.com/leolani/cltl-emissor-data.git
cd cltl-emissor-data
pip install -e .
```

### With optional dependencies

```bash
pip install cltl.emissor-data[impl,service,client]
```

- `impl`: Audio/image processing (soundfile, opencv-python)
- `service`: HTTP service (flask)
- `client`: HTTP client (requests)

## Configuration

Add to your application configuration file (e.g., `config/default.config`):

```ini
[cltl.emissor-data]
path: ./storage/emissor
flush_interval: 1

[cltl.emissor-data.event]
topics: cltl.topic.scenario
        cltl.topic.text_in
        cltl.topic.text_out
        cltl.topic.image
        cltl.topic.microphone
```

### Configuration Parameters

- **path**: Directory where scenario data will be stored
- **flush_interval**: Seconds between data flushes to disk (-1 to disable, 0 for immediate)
- **topics**: Event bus topics to subscribe to for data collection

## Usage

### In a Python Application

```python
from cltl.combot.infra.config import ConfigurationManager
from cltl.combot.infra.event import EventBus
from cltl.combot.infra.resource import ResourceManager
from cltl.emissordata.file_storage import EmissorDataFileStorage
from cltl_service.emissordata.service import EmissorDataService

# Initialize storage
config_manager = ConfigurationManager()
storage = EmissorDataFileStorage.from_config(config_manager)

# Create service
event_bus = EventBus()
resource_manager = ResourceManager()
service = EmissorDataService.from_config(
    storage, event_bus, resource_manager, config_manager
)

# Start service
service.start()

# Get Flask app for HTTP endpoints
app = service.app
```

### Storage Structure

Data is organized on disk as follows:

```
storage/emissor/
├── <scenario-uuid-1>/
│   ├── <scenario-uuid-1>.json    # Scenario metadata
│   ├── text.json                  # Text signals
│   ├── audio.json                 # Audio signals
│   ├── image.json                 # Image signals
│   ├── rdf/                       # Brain logs (if available)
│   ├── audio/                     # Audio files
│   └── image/                     # Image files
└── <scenario-uuid-2>/
    └── ...
```

## HTTP Endpoints

The service provides HTTP endpoints for accessing stored scenario data. These endpoints are automatically mounted at `/emissor` when integrated into an application.

### List All Scenarios

```http
GET /emissor/scenarios
```

**Response** (200 OK):
```json
{
  "scenarios": [
    {
      "id": "scenario-uuid",
      "start": 1761773779254,
      "end": 1761774167599,
      "context": {
        "agent": {"uri": "...", "name": "Leolani"},
        "speaker": {"uri": "...", "name": "Human"}
      }
    }
  ]
}
```

Returns a list of all archived scenarios with basic metadata (ID, timestamps, context).

---

### Get Scenario Data

```http
GET /emissor/scenarios/<scenario_id>
```

**Parameters**:
- `scenario_id`: UUID of the scenario (alphanumeric, dash, underscore only)

**Response** (200 OK):
```json
{
  "scenario": {
    "id": "scenario-uuid",
    "ruler": {"start": 1761773779254, "end": 1761774167599},
    "context": {...},
    "signals": {...}
  },
  "signals": {
    "text": [...],
    "audio": [...],
    "image": [...]
  }
}
```

Returns complete scenario data including all signals and mentions for all modalities.

**Error Responses**:
- `400 Bad Request`: Invalid scenario_id format
- `404 Not Found`: Scenario does not exist
- `500 Internal Server Error`: Server error

---

### Download Scenario as Zip

```http
GET /emissor/scenarios/<scenario_id>/download
```

**Parameters**:
- `scenario_id`: UUID of the scenario

**Response** (200 OK):
- Content-Type: `application/zip`
- Content-Disposition: `attachment; filename="<scenario_id>.zip"`
- Body: Binary zip file containing all scenario data (JSON files, audio/, image/, rdf/)

Downloads a complete scenario including all JSON metadata files, audio files, image files, and RDF logs.

**Size Limit**: Maximum 500MB per scenario (configurable via `MAX_ZIP_SIZE_MB` constant)

**Error Responses**:
- `400 Bad Request`: Invalid scenario_id format
- `404 Not Found`: Scenario does not exist
- `500 Internal Server Error`: Server error or size limit exceeded

---

### Access Static Files

```http
GET /emissor/storage/<scenario_id>/<file_path>
```

**Examples**:
- `/emissor/storage/scenario-uuid/text.json` - Get text signals
- `/emissor/storage/scenario-uuid/scenario-uuid.json` - Get scenario metadata
- `/emissor/storage/scenario-uuid/rdf/brain_log_2025-11-11.trig` - Get RDF logs

Direct file access to any file within a scenario directory. Useful for selective downloads or viewing individual files.

---

### Get Scenario ID for Element

```http
GET /emissor/<element_id>/scenario/id
```

**Parameters**:
- `element_id`: ID of a signal, mention, or annotation

**Response** (200 OK):
```
scenario-uuid
```

Returns the scenario ID that contains the given element. Only works for currently active scenarios in memory.

**Error Responses**:
- `404 Not Found`: Element not found in active scenarios

---

### Usage Examples

#### Using cURL

```bash
# List all scenarios
curl http://localhost:8000/emissor/scenarios

# Get specific scenario
curl http://localhost:8000/emissor/scenarios/12345678-1234-1234-1234-123456789abc

# Download scenario as zip
curl -O -J http://localhost:8000/emissor/scenarios/12345678-1234-1234-1234-123456789abc/download

# Access specific file
curl http://localhost:8000/emissor/storage/12345678-1234-1234-1234-123456789abc/text.json
```

#### Using wget (Recursive Download)

```bash
# Download entire scenario directory recursively
wget -r -np -nH --cut-dirs=2 http://localhost:8000/emissor/storage/12345678-1234-1234-1234-123456789abc/
```

#### Using Python

```python
import requests

# List scenarios
response = requests.get('http://localhost:8000/emissor/scenarios')
scenarios = response.json()['scenarios']

# Get scenario data
scenario_id = scenarios[0]['id']
response = requests.get(f'http://localhost:8000/emissor/scenarios/{scenario_id}')
scenario_data = response.json()

# Download scenario
response = requests.get(
    f'http://localhost:8000/emissor/scenarios/{scenario_id}/download',
    stream=True
)
with open(f'{scenario_id}.zip', 'wb') as f:
    for chunk in response.iter_content(chunk_size=8192):
        f.write(chunk)
```

---

### Security Features

The HTTP endpoints implement multiple security measures:

- **Input validation**: Scenario IDs must be alphanumeric with dash/underscore only (max 255 chars)
- **Path traversal protection**: Two-layer defense prevents access outside storage directory
- **Size limits**: Zip downloads limited to 500MB to prevent resource exhaustion
- **Error sanitization**: Generic error messages prevent information leakage
- **No caching**: Cache-Control headers prevent sensitive data caching

**Blocked patterns**:
- Path traversal: `../../../etc/passwd`
- Special characters: `@!#$%^&*()`
- Absolute paths: `/etc/passwd`
- Null bytes and control characters

## API

### EmissorDataStorage

Abstract interface for EMISSOR data storage implementations.

**Key Methods**:
- `start_scenario(scenario)`: Begin tracking a new scenario
- `stop_scenario(scenario)`: Stop tracking and finalize a scenario
- `add_signal(signal)`: Add a signal (text, audio, image)
- `add_mentions(mentions)`: Add mentions with annotations
- `list_scenarios()`: List all stored scenarios
- `get_scenario(scenario_id)`: Retrieve complete scenario data
- `create_scenario_zip(scenario_id)`: Create downloadable zip archive
- `flush()`: Persist in-memory data to disk

### EmissorDataFileStorage

File-based implementation of EmissorDataStorage.

**Features**:
- Multi-scenario support (concurrent conversations)
- Periodic flushing to disk
- Signal indexing for fast lookup
- Audio/image file handling
- RDF log integration

## Service

### EmissorDataService

Service that integrates EMISSOR data storage with the event bus and provides HTTP endpoints.

**Responsibilities**:
- Subscribe to event topics for data collection
- Handle scenario lifecycle events (start, update, stop)
- Process signals and mentions from events
- Provide Flask app with REST endpoints
- Manage periodic data flushing

**Event Topics**:
- `ScenarioStarted`: Initialize new scenario
- `ScenarioStopped`: Finalize and archive scenario
- `ScenarioEvent`: Update scenario metadata
- Signal topics: Capture text, audio, image signals
- Mention topics: Capture annotations

## Development

### Running Tests

```bash
# Install development dependencies
pip install -e ".[impl,service,client]"
pip install pytest pytest-cov

# Run tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=cltl --cov=cltl_service --cov-report=html
```

### Project Structure

```
cltl-emissor-data/
├── src/
│   ├── cltl/
│   │   └── emissordata/
│   │       ├── api.py              # Abstract storage interface
│   │       ├── file_storage.py     # File-based implementation
│   │       └── container.py        # Dependency injection
│   └── cltl_service/
│       └── emissordata/
│           └── service.py          # Flask service with endpoints
├── tests/
│   ├── test_file_storage.py       # Storage implementation tests
│   └── test_service.py            # HTTP endpoint tests
├── config/
│   └── default.config             # Default configuration
└── setup.py                       # Package definition
```

## Contributing

Contributions are what make the open source community such an amazing place to be learn, inspire, and create. Any contributions you make are **greatly appreciated**.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

Distributed under the MIT License. See [`LICENSE`](https://github.com/leolani/cltl-combot/blob/main/LICENCE) for more information.

## Authors

* [Taewoon Kim](https://tae898.github.io/)
* [Thomas Baier](https://www.linkedin.com/in/thomas-baier-05519030/)
* [Selene Báez Santamaría](https://selbaez.github.io/)
* [Piek Vossen](https://github.com/piekvossen)
