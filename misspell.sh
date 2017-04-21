#!/bin/sh
if [ "$(misspell -locale US . | grep -v 'vendor/')" ]; then
  misspell -locale US . | grep -v "vendor/"
  exit 2
fi
