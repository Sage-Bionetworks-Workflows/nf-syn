#!/usr/bin/env nextflow
nextflow.enable.dsl=2

// print a Synapse's entity file content
file('syn://abc')
    .readLines()
    .each { println it }
