# libtorrent4j

[![macOS](https://github.com/aldenml/libtorrent4j/workflows/macOS/badge.svg)](https://github.com/aldenml/libtorrent4j/actions?query=workflow%3AmacOS)
[![Linux](https://github.com/aldenml/libtorrent4j/workflows/Linux/badge.svg)](https://github.com/aldenml/libtorrent4j/actions?query=workflow%3ALinux)
[![Windows](https://github.com/aldenml/libtorrent4j/workflows/Windows/badge.svg)](https://github.com/aldenml/libtorrent4j/actions?query=workflow%3AWindows)
[![Android](https://github.com/aldenml/libtorrent4j/workflows/Android/badge.svg)](https://github.com/aldenml/libtorrent4j/actions?query=workflow%3AAndroid)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/5dda1f00528e4d93864eb8694c702bba)](https://app.codacy.com/manual/aldenml/libtorrent4j/dashboard)
[![Maven Central](https://img.shields.io/maven-central/v/org.libtorrent4j/libtorrent4j.svg?label=maven)](https://central.sonatype.com/search?namespace=org.libtorrent4j)

A swig Java interface for [libtorrent](https://github.com/arvidn/libtorrent).

| Features |   |
|---|---|
| Support for WebTorrent | https://webtorrent.io |
| Sequential downloading |  |
| Torrents queueing and prioritizing |  |
| Torrent content selection and prioritizing | |
| NAT-PMP and UPnP support | |
| Fast resume support | |
| HTTP proxies and basic authentication | |
| IP filter | |
| Torrents over SSL |  |
| Android Storage Access Framework (SAF) disk backend | Fork release `v2.1.0-39-saf.1` |
| `lt_donthave` extension | [BEP 54](https://www.bittorrent.org/beps/bep_0054.html) |
| Magnet URI extension - specify indices to download | [BEP 53](https://www.bittorrent.org/beps/bep_0053.html) |
| BitTorrent Protocol v2 | [BEP 52](https://www.bittorrent.org/beps/bep_0052.html) |
| DHT Infohash Indexing | [BEP 51](https://www.bittorrent.org/beps/bep_0051.html) |
| Tracker Protocol Extension: Scrape | [BEP 48](https://www.bittorrent.org/beps/bep_0048.html) |
| Padding files and attributes | [BEP 47](https://www.bittorrent.org/beps/bep_0047.html) |
| Multiple-address operation for the DHT | [BEP 45](https://www.bittorrent.org/beps/bep_0045.html) |
| Storing arbitrary data in the DHT | [BEP 44](https://www.bittorrent.org/beps/bep_0044.html) |
| Read-only DHT Nodes | [BEP 43](https://www.bittorrent.org/beps/bep_0043.html) |
| IPv6 extension for DHT | [BEP 32](https://www.bittorrent.org/beps/bep_0032.html) |
| uTorrent transport protocol (uTP) | [BEP 29](https://www.bittorrent.org/beps/bep_0029.html) |
| Private Torrents | [BEP 27](https://www.bittorrent.org/beps/bep_0027.html) |
| Tracker Returns External IP | [BEP 24](https://www.bittorrent.org/beps/bep_0024.html) |
| Tracker Returns Compact Peer Lists | [BEP 23](https://www.bittorrent.org/beps/bep_0023.html) |
| Extension for Partial Seeds | [BEP 21](https://www.bittorrent.org/beps/bep_0021.html) |
| HTTP/FTP Seeding (GetRight-style) | [BEP 19](https://www.bittorrent.org/beps/bep_0019.html) |
| Superseeding | [BEP 16](https://www.bittorrent.org/beps/bep_0016.html) |
| UDP Tracker Protocol | [BEP 15](https://www.bittorrent.org/beps/bep_0015.html) |
| Local Service Discovery (LSD) | [BEP 14](https://www.bittorrent.org/beps/bep_0014.html) |
| Multitracker Metadata Extension | [BEP 12](https://www.bittorrent.org/beps/bep_0012.html) |
| Peer Exchange (PEX) | [BEP 11](https://www.bittorrent.org/beps/bep_0011.html) |
| Extension Protocol | [BEP 10](https://www.bittorrent.org/beps/bep_0010.html) |
| Magnet links | [BEP 9](https://www.bittorrent.org/beps/bep_0009.html) |
| IPv6 Tracker Extension | [BEP 7](https://www.bittorrent.org/beps/bep_0007.html) |
| Distributed hash table (DHT) | [BEP 5](https://www.bittorrent.org/beps/bep_0005.html) |

## Using

Download [the latest JAR](https://search.maven.org/classic/remote_content?g=org.libtorrent4j&a=libtorrent4j&v=LATEST) or get the dependency via Maven:
```xml
<dependency>
  <groupId>org.libtorrent4j</groupId>
  <artifactId>libtorrent4j</artifactId>
  <version>2.x.x</version>
</dependency>
```
or Gradle:
```groovy
compile 'org.libtorrent4j:libtorrent4j:2.x.x'
```

If you use ProGuard to obfuscate/minify make sure to add the following statement

`-keep class org.libtorrent4j.swig.libtorrent_jni {*;}`

Note that there are multiple versions of libtorrent4j for different platforms:
```
libtorrent4j
libtorrent4j-android-<arch>
libtorrent4j-macos
libtorrent4j-linux
libtorrent4j-windows
```
These are all different artifacts, you need to select according to your architecture.

For examples look at [demos](https://github.com/aldenml/libtorrent4j/tree/master/demo)
and [tests](https://github.com/aldenml/libtorrent4j/tree/master/src/test/java/org/libtorrent4j).

Architectures supported:

- Android (armeabi-v7a, arm64-v8a, x86, x86_64)
- macOS (arm64)
- Linux (x86_64)
- Windows (x86_64)

## Android Storage Access Framework (SAF)

This fork supports downloading directly into an Android Storage Access Framework
document tree. SAF support is available starting with
`v2.1.0-39-saf.1`, which is based on upstream libtorrent4j `v2.1.0-39`.
The feature is fork-specific and is not included in the upstream Maven Central
artifacts.

The SAF backend is intended for Android API 24 and newer and has been verified
for `armeabi-v7a`, `arm64-v8a`, `x86`, and `x86_64`. Build the core JAR and
the Android ABI JARs from this tag; do not combine the Java classes from this
fork with native libraries from upstream or another version.

### Integration outline

1. Let the user select a directory with `ACTION_OPEN_DOCUMENT_TREE` and persist
   both the read and write URI permissions.
2. Implement `org.libtorrent4j.swig.flow_saf_storage` using
   `ContentResolver`, `DocumentsContract`, or `DocumentFile` beneath that tree.
3. Install the bridge on `SessionParams` before starting `SessionManager`.
4. Keep the bridge strongly reachable for the complete native session lifetime.
5. Stop the session before calling `delete()` on the bridge.

```kotlin
val storageBridge = AndroidSafStorageBridge(contentResolver, persistedTreeUri)
val params = SessionParams()

params.swig().set_flow_saf_disk_io_constructor(storageBridge)

val sessionManager = SessionManager()
sessionManager.start(params)

// Keep storageBridge alive while sessionManager is running.
// During shutdown:
sessionManager.stop()
storageBridge.delete()
```

Use `.flow-session-root` as the torrent save-path sentinel when the selected SAF
tree itself is the download root. All paths delivered to the bridge are UTF-8,
forward-slash-separated relative paths beneath that root. Reject empty
components, `.` and `..`, backslashes, absolute paths, and any path that could
escape the granted tree.

### Bridge callback contract

| Callback | Contract |
|---|---|
| `validate_path(path)` | Pure validation only; do not perform provider I/O. Return `0` or a negative POSIX errno. |
| `create_directory(path)` | Create the relative directory and any provider state required for it. |
| `open_file(path, mode)` | Return a seekable file descriptor. Mode `0` is read-only and mode `2` is read/write. Return a negative errno on failure. |
| `file_size(path)` | Return the size in bytes, or a negative errno. |
| `rename(oldPath, newPath)` | Rename or move within the granted tree without allowing traversal outside it. |
| `remove(path, recursive)` | Delete a file or directory; honor `recursive` for directory trees. |

For `open_file`, detach the descriptor from `ParcelFileDescriptor` before
returning it, for example with `detachFd()`. Ownership then transfers to the
native disk backend, which closes the descriptor. The descriptor must support
`pread`, `pwrite`, and `ftruncate`; cloud-only providers that do not expose a
seekable descriptor are not compatible.

Provider callbacks run on libtorrent's disk worker thread, not the Android main
thread. Convert provider exceptions into negative errno values such as
`-ENOENT`, `-EACCES`, `-ENOSPC`, or `-EIO`. A revoked URI permission should be
treated as a storage failure: stop or pause affected torrents, request the tree
grant again, rebuild the bridge, and start a new native session.

This source tag does not imply publication to Maven Central. Applications can
consume locally built JARs with Gradle `files(...)` dependencies or publish the
matching artifacts to their own Maven repository. Package all required Android
ABI JARs in the APK or restrict the APK's ABI filters accordingly.

#### About stability

This library tracks libtorrent [`master`](https://github.com/arvidn/libtorrent/tree/master) branch.
The branch is very stable, runs a lot of tests, and receives bug fixes quickly.

## Android local builds

It's possible to build android binaries locally. The solution is docker based, you
need to have Docker installed and running (see https://docs.docker.com/engine/install/).

Go to the folder `swig/android-build` and perform all the operations inside it.

1 - Build the docker image just one time (takes a long time):
```
docker build -t lt4j:latest .
```

2 - Select your architecture and run the build script, for example:
```
./build-arm.sh
```

3 - Collect the jars in `build/libs` at the root of the project. Repeat
the step 2) for the desired architectures.

## License

Licensed under the terms of the MIT license, available [here](LICENSE.md).
