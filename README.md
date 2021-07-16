# BeyondFS

A high-performance, POSIX-ish File System based on [beyondstorage/go-storage](https://github.com/beyondstorage/go-storage).

## Design

- Only cache metadata
- Sharable
- POSIX-ish

Refer to [RFC-5: BeyondFS Design](./docs/rfcs/5-beyond-fs-design.md) to know more. 

## Current Status

We are working on [implement a POSIX-ish file system that only caches metadata locally](https://github.com/beyondstorage/beyond-fs/issues/8)
