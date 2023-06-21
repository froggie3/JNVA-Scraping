# JNVA-Scraping

A few scripts which helps you to retrieve all the JSON-formatted threads on なんJNVA部.

## Updates

__2023/6/19: Complete rewrite!__
Things gonna be done with Python altogether!

## How to Prepare?

Setup Python venv.

|Shell|Command to enable venv|
|:----|:----|
|bash/zsh|`$ source <venv>/bin/activate`|
|fish|`$ source <venv>/bin/activate.fish`|
|csh/tcsh|`$ source <venv>/bin/activate.csh`|
|PowerShell|`$ <venv>/bin/Activate.ps1`|
|cmd.exe|`C:\> <venv>\Scripts\activate.bat`|
|PowerShell|`PS C:\> <venv>\Scripts\Activate.ps1`|

We use this package managing method for this tool, so then install this:

https://github.com/mitsuhiko/rye

and then,

```bash
rye sync
```

## Before you start

### Setting the environmental variable

you need to set the environmental variable for this program.
```
# change as you prefer
echo 'export JNVADB_PATH=$HOME/.jnvadb.sqlite3' >> $HOME/.bashrc
export JNVADB_PATH=$HOME/.jnvadb.sqlite3
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
