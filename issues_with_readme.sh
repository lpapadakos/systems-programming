#!/bin/sh

# For some reason .cshrc has a weird LC_CTYPE set for all of us.
# Clear that.
env LC_CTYPE= less README
