#!/usr/bin/env nextflow
nextflow.enable.dsl=2

// print a Synapse's entity file content
file('syn://syn33295438')
    .readLines()
    .each { println it }
