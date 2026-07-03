# WebDAV

Build a WebDAV server with Delta-V versioning and WebDAV Search.

## Features

* When accessed with a browser, render `.md` files as HTML via a Markdown renderer.
* Support multiple versions of a document.
* Maintain a reverse index for searching documents by query terms.

## Objective

Objective of the project is to build a personal storage server that is accessible both from a browser and via WebDAV clients, with support for Markdown, images, and videos.

## Implementation

* Content should be stored in a content-addressable blob store and virtualize the folders and files to the client.
* For larger files, use FastCDC to chunk the content, store unique chunks in the blob store.
* Track the version history of files as they are modified.

## References

* [Delta-V](https://www.rfc-editor.org/rfc/rfc3253)
* [WebDAV Search](https://www.rfc-editor.org/rfc/rfc5323)
* http://www.webdav.org
* https://en.wikipedia.org/wiki/WebDAV
* https://github.com/fstanis/awesome-webdav
