Windows builds by Jeroen Ooms, 2014.
Adapted from: https://gist.github.com/ant32/ad40af3b5fec652f335b
Downloaded the standard CMAKE windows executables and copied to into the MSYS dir.
Current Rtools gcc didn't work, so I used mingw-w64 version 4.9.2 on both i386 and x64.

To build:

cmake -G "MSYS Makefiles" . -DCMAKE_BUILD_TYPE=Release
make
