# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.1] - 2024-04-21

### Added

- Add monitor to check why it is slow
- Add otpl
- Add access test after set_attr
- Add quick start with virtual machine

### Changed

- How to alloc data?
- Write will dead lock
- Implement write back logic
- Figure out why we get stuck
- We can rmdir
- Get rid of dashmap on handle table
- Refact openfile cache
- Implement truncate function when set_attr change the file length
- Implement link; TODO: read link
- Implement basic link and unlink
- Implement rename
- Implement vfs:symlink
- Implement symlink & readlink
- Update licenserc
- Fmt
- Try1
- Try2
- Try3
- Try4

### Fixed

- Read & truncate read length after write
- Fix vfs::truncate
- 1. attr mode; 2. link; 3. unlink


<!-- generated by git-cliff -->
