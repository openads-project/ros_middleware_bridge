# ros_middleware_bridge

<p align="center">
  <a href="https://github.com/openads-project"><img src="https://img.shields.io/badge/OpenADS-f5ff01"/></a>
  <a href="https://www.ros.org"><img src="https://img.shields.io/badge/ROS 2-jazzy-22314e"/></a>
  <a href="https://github.com/openads-project/ros_middleware_bridge/releases/latest"><img src="https://img.shields.io/github/v/release/openads-project/ros_middleware_bridge"/></a>
  <a href="https://github.com/openads-project/ros_middleware_bridge/blob/main/LICENSE"><img src="https://img.shields.io/github/license/openads-project/ros_middleware_bridge"/></a>
  <br>
  <a href="https://github.com/openads-project/ros_middleware_bridge/actions/workflows/docker-ros.yml"><img src="https://github.com/openads-project/ros_middleware_bridge/actions/workflows/docker-ros.yml/badge.svg"/></a>
  <a href="https://github.com/openads-project/ros_middleware_bridge/actions/workflows/compose-oci.yml"><img src="https://github.com/openads-project/ros_middleware_bridge/actions/workflows/compose-oci.yml/badge.svg"/></a>
  <a href="https://openads-project.github.io/ros_middleware_bridge"><img src="https://github.com/openads-project/ros_middleware_bridge/actions/workflows/docs.yml/badge.svg"/></a>
  <a href="https://github.com/openads-project/ros_middleware_bridge/actions/workflows/consistency.yml"><img src="https://github.com/openads-project/ros_middleware_bridge/actions/workflows/consistency.yml/badge.svg"/></a>
</p>

**ROS 2 middleware bridge serializing topic data between different RMW implementations**

The `ros_middleware_bridge` node forwards ROS 2 topic data between two endpoint
processes that can run with different `RMW_IMPLEMENTATION` values. Each endpoint
subscribes to configured local topics, serializes the messages as
`rclcpp::SerializedMessage`, transfers them via UDP or shared memory, and
republishes them on the opposite side with the original ROS message type. The
default launch file starts two instances of the same node, one for side A and
one for side B, so data can be bridged in both directions between middleware
domains such as Fast DDS and Zenoh.

<p align="center">
  <strong>🚀 <a href="#-quick-start">Quick Start</a></strong> • <strong>💻 <a href="#-development">Development</a></strong> • <strong>📝 <a href="#-documentation">Documentation</a></strong>
</p> 

> [!IMPORTANT]
> This repository is part of [***OpenADS***](https://github.com/openads-project), the *Open Automated Driving Systems* project. *OpenADS* and its modules have been initiated and are currently being maintained by the [**Institute for Automotive Engineering (ika) at RWTH Aachen University**](https://www.ika.rwth-aachen.de/de/).

## 🚀 Quick Start

1. Start a container of the pre-built runtime image.
    ```bash
    docker run --rm -it ghcr.io/openads-project/ros_middleware_bridge:latest bash
    ```
1. Inside the container, launch the pre-built nodes.
    ```bash
    ros2 launch ros_middleware_bridge ros_middleware_bridge.launch.py
    ```

## 💻 Development

### Set up Development Environment

1. Clone the repository.
    ```bash
    git clone https://github.com/openads-project/ros_middleware_bridge.git
    ```
1. Initialize the [`.openads-dev-environment`](https://github.com/openads-project/openads-dev-environment) submodule containing development environment configuration.
    ```bash
    cd ros_middleware_bridge
    git submodule update --init --recursive
    ```
1. Open the repository in [Visual Studio Code](https://code.visualstudio.com).
    ```bash
    code .
    ```
1. Install the recommended VS Code extensions.
    > *Ctrl+Shift+P / Extensions: Show Recommended Extensions / Install Workspace Recommended Extensions (Cloud Download Icon)*
1. Reopen the repository in a [Dev Container](https://code.visualstudio.com/docs/devcontainers/containers).
    > *Ctrl+Shift+P / Dev Containers: Rebuild and Reopen in Container*

### Build

> *Ctrl+Shift+B*

```bash
colcon build
```

### Run Tests

> *Ctrl+Shift+P / Tasks: Run Test Task*

```bash
colcon build --cmake-args -DCMAKE_EXPORT_COMPILE_COMMANDS=1
colcon test
colcon test-result --verbose
```


## 📝 Documentation

Package and node interfaces are documented in the respective package READMEs listed below. Implementation details are found in the [Source Code Documentation](https://openads-project.github.io/ros_middleware_bridge).

| Package | Description |
| --- | --- |
| [ros_middleware_bridge](ros_middleware_bridge/README.md) | ROS 2 middleware bridge serializing topic data between different RMW implementations |

## ⚖️ Licensing

The source code in this repository is licensed under Apache-2.0, see [LICENSE](LICENSE). Container images provided by this repository may contain third-party software shipped with their own license terms.

## 🙏 Acknowledgements

Development and maintenance of this repository are supported by the following projects. We acknowledge the funding of the respective institutions.

| Project | Funding Institution | Grant Number |
| --- | --- | --- |
| [AIGGREGATE](https://aiggregate.eu/) | 🇪🇺 European Union | 101202457 |

<p>
  <img src="https://www.drought.uni-freiburg.de/stressres/images/bmftr-logo/image" height=70>
  <img src="https://ec.europa.eu/regional_policy/images/information-sources/logo-download-center/eu_funded_en.jpg" height=70>
</p>

<sub><sup>Funded by the European Union. Views and opinions expressed are however those of the author(s) only and do not necessarily reflect those of the European Union or the European Climate, Infrastructure and Environment Executive Agency (CINEA). Neither the European Union nor CINEA can be held responsible for them.</sup></sub>
