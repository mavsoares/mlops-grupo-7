#!/bin/bash

git add -u
git add .
git commit -m "$1"
git push
git status

