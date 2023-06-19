# JNVA-Scraping

A few scripts which helps you to retrieve all the JSON-formatted threads on なんJNVA部.

## Updates

__2023/6/19: Complete rewrite!__
Things gonna be done with Python altogether!

## How to Prepare?

Setup Python venv.

|Shell|Command to enable venv| |
|:----|:----|:----|
|bash/zsh|$ source <venv>/bin/activate| |
|fish|$ source <venv>/bin/activate.fish| |
|csh/tcsh|$ source <venv>/bin/activate.csh| |
|PowerShell|$ <venv>/bin/Activate.ps1| |
|cmd.exe|C:\> <venv>\Scripts\activate.bat| |
|PowerShell|PS C:\> <venv>\Scripts\Activate.ps1| |

We use this package managing method for this tool, so then install this:

https://github.com/mitsuhiko/rye

and then,

```bash
rye sync
```

## How to Use

before you start, create database (once you do, forget it!)
```bash
python database_helper.py
```
And then
```bash
python downloader.py
```
for more information,
```
python downloader.py --help
```

### Setting the environmental variable

for your preference, you can set the environmental variable for this program.
```
JNAIDB_DIR=$HOME/db.db
```

## Requisites

* Python (tested on `v3.11.3`)
