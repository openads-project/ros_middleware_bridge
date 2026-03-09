#!/usr/bin/env python3

import os

from ament_index_python import get_package_share_directory
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node, SetParameter


def generate_launch_description():
    package_share = get_package_share_directory("middleware_bridge")
    default_params = os.path.join(package_share, "config", "params.yml")

    launch_args = [
        DeclareLaunchArgument("namespace", default_value="", description="node namespace"),
        DeclareLaunchArgument("log_level", default_value="info", description="ROS logging level (debug, info, warn, error, fatal)"),
        DeclareLaunchArgument("use_sim_time", default_value="false", description="use simulation clock"),
    ]

    nodes = [
        Node(
            package="middleware_bridge",
            executable="middleware_bridge",
            namespace=LaunchConfiguration("namespace"),
            name="bridge_fast",
            parameters=[default_params],
            arguments=["--ros-args", "--log-level", LaunchConfiguration("log_level")],
            additional_env={"RMW_IMPLEMENTATION": "rmw_fastrtps_cpp"},
            output="screen",
            emulate_tty=True,
        ),
        Node(
            package="middleware_bridge",
            executable="middleware_bridge",
            namespace=LaunchConfiguration("namespace"),
            name="bridge_zenoh",
            parameters=[default_params],
            arguments=["--ros-args", "--log-level", LaunchConfiguration("log_level")],
            additional_env={"RMW_IMPLEMENTATION": "rmw_zenoh_cpp"},
            output="screen",
            emulate_tty=True,
        ),
    ]

    return LaunchDescription([
        *launch_args,
        SetParameter("use_sim_time", LaunchConfiguration("use_sim_time")),
        *nodes,
    ])
