cmake_minimum_required(VERSION 3.14)
project(utree)

add_library(utree "")
target_sources(utree PUBLIC
        "utree.cc"
        )
target_include_directories(utree PUBLIC .)
