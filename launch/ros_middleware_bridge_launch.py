#!/usr/bin/env python3

import os

from ament_index_python import get_package_share_directory
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node, SetParameter
from launch_ros.parameter_descriptions import ParameterValue


def generate_launch_description():
    package_share = get_package_share_directory("ros_middleware_bridge")
    default_params = os.path.join(package_share, "config", "params.yml")

    launch_args = [
        DeclareLaunchArgument("params_file", default_value=default_params, description="parameter file path"),
        DeclareLaunchArgument("namespace", default_value="", description="node namespace"),
        DeclareLaunchArgument("log_level", default_value="info", description="ROS logging level (debug, info, warn, error, fatal)"),
        DeclareLaunchArgument("use_sim_time", default_value="false", description="use simulation clock"),
        DeclareLaunchArgument("side_a_name", default_value="bridge_fast", description="side A node name (default Fast DDS side)"),
        DeclareLaunchArgument("side_b_name", default_value="bridge_zenoh", description="side B node name (default Zenoh side)"),
        DeclareLaunchArgument(
            "side_a_rmw_implementation",
            default_value="rmw_fastrtps_cpp",
            description="RMW_IMPLEMENTATION for side A (project default: Fast DDS)",
        ),
        DeclareLaunchArgument(
            "side_b_rmw_implementation",
            default_value="rmw_zenoh_cpp",
            description="RMW_IMPLEMENTATION for side B (project default: Zenoh)",
        ),
        DeclareLaunchArgument("side_a_tx_port", default_value="17001", description="UDP transmit port for side A"),
        DeclareLaunchArgument("side_a_rx_port", default_value="17002", description="UDP receive port for side A"),
        DeclareLaunchArgument("side_b_tx_port", default_value="17002", description="UDP transmit port for side B"),
        DeclareLaunchArgument("side_b_rx_port", default_value="17001", description="UDP receive port for side B"),
    ]

    nodes = [
        Node(
            package="ros_middleware_bridge",
            executable="ros_middleware_bridge",
            namespace=LaunchConfiguration("namespace"),
            name=LaunchConfiguration("side_a_name"),
            parameters=[
                LaunchConfiguration("params_file"),
                {
                    "bridge_side": "a",
                    "tx_port": ParameterValue(LaunchConfiguration("side_a_tx_port"), value_type=int),
                    "rx_port": ParameterValue(LaunchConfiguration("side_a_rx_port"), value_type=int),
                },
            ],
            arguments=["--ros-args", "--log-level", LaunchConfiguration("log_level")],
            additional_env={"RMW_IMPLEMENTATION": LaunchConfiguration("side_a_rmw_implementation")},
            output="screen",
            emulate_tty=True,
        ),
        Node(
            package="ros_middleware_bridge",
            executable="ros_middleware_bridge",
            namespace=LaunchConfiguration("namespace"),
            name=LaunchConfiguration("side_b_name"),
            parameters=[
                LaunchConfiguration("params_file"),
                {
                    "bridge_side": "b",
                    "tx_port": ParameterValue(LaunchConfiguration("side_b_tx_port"), value_type=int),
                    "rx_port": ParameterValue(LaunchConfiguration("side_b_rx_port"), value_type=int),
                },
            ],
            arguments=["--ros-args", "--log-level", LaunchConfiguration("log_level")],
            additional_env={"RMW_IMPLEMENTATION": LaunchConfiguration("side_b_rmw_implementation")},
            output="screen",
            emulate_tty=True,
        ),
    ]

    return LaunchDescription([
        *launch_args,
        SetParameter("use_sim_time", LaunchConfiguration("use_sim_time")),
        *nodes,
    ])
