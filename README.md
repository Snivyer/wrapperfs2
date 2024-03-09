# wrapperfs2
wrapperfs2 is the new version of wrappers.

2024-2-28: I finished the major functions of the wrapperfs. It is running like other file systems and I feel happy.


2024-2-29: I add io static and operations and find performances of the wrapperfs are unsatisfacting. I want to add cache to improve it.

2024-3-1 add location, entries, relation cache and metadata cache.

2024-3-5 serializing and deserializing KV pair using Json cause degradated performance, so I improve the process of serializing and deserializing and commit it to the new branch: branck-1.

2024-3-6 opendir should not execute the full process of PathLookup() and just need to call the WrapperLookup().


2024-3-9 add read_only_cache.