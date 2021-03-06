EPaxos
======

Original EPaxos implementation adjusted for benchmarks described in [1].
Protobuf encoding generated by our modified
[version](https://github.com/relab/protobuf/tree/epaxos) of gogoprotobuf to
conform to the original EPaxos `fastrpc` interface.

### Branches

* baseline
* protobuf-16b 
* protobuf-1kb 
* gorums-16b
* gorums-1kb

### References

[1] Tormod Erevik Lea, Leander Jehl, and Hein Meling.
    _Towards New Abstractions for Implementing Quorum-based Systems._
    In 37th International Conference on Distributed Computing Systems (ICDCS), Jun 2017.

### Original Authors

Copyright 2013 Carnegie Mellon University

Iulian Moraru, David G. Andersen -- Carnegie Mellon University

Michael Kaminsky -- Intel Labs
