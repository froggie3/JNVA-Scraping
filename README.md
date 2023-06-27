# JNVA-Scraping

A script which helps you retrieve all the threads, and retain all the backup for NJVA.

## Updates

__2023/6/24__: `.env` file was introduced to give us a ability to set the location for the database to contain an archive for threads.

__2023/6/19: Complete rewrite!__
Things gonna be done with Python altogether!

## How to get started?

First, clone this repository, and change a current directory to where it was cloned.

If you are sure to the rye installed on your computer, then type this to install requirements.

```bash
rye sync
```

Note: in this project, __rye__ is used as packages managing tools, which is really comprehensive and useful for managing the project, so then please before install this if you have not installed yet:

https://github.com/mitsuhiko/rye

Setup a virtual environment for Python to work.

|Shell|Command to enable venv|
|:----|:----|
|bash/zsh|`$ source <venv>/bin/activate`|
|fish|`$ source <venv>/bin/activate.fish`|
|csh/tcsh|`$ source <venv>/bin/activate.csh`|
|PowerShell|`$ <venv>/bin/Activate.ps1`|
|cmd.exe|`C:\> <venv>\Scripts\activate.bat`|
|PowerShell|`PS C:\> <venv>\Scripts\Activate.ps1`|


## Before you start

### Setting the environmental variable

You have to create `.env` file into the root directory of this repository directory for this program, where it should be on the same place as this README.md is located, or which is in the parent directory of `src/`.

And then, you can write like this:

```shell
# for the directory of database file, you have the right to change!
JNVADB_PATH=/your/path/.to_the_database.sqlite3
```

or you can manually set the environmental variable. 

However, note that in this case, you CANNOT run this script with cron, which really makes it difficult to import environment variables.

```shell
echo 'export JNVADB_PATH=/your/path/.to_the_database.sqlite3' >> $HOME/.bashrc
source $HOME/.bashrc
```

### Create database

once you do, you can forget it!

```bash
python3 database_helper.py
```

## How to Use

for more information : `--help`

```bash
python3 downloader.py
```

## Requisites

* Python (tested on `v3.11.3`)
