# wsprnet-scraper
Daemon which uses the Wsprnet API to scrape new spots and adds them to local Timescale Database

## Usage

The scripts are meant to be used in sequence:

```
wsprnet-scraper.sh -> wsprnet_azi_calc.py -> ts_upload_batch.py
```

### `wsprnet_azi_calc.py`

```
wsprnet_azi_calc.py [-h] -i FILE -o [FILE]
```

`STDIN` and `STDOUT` can be used instead of files on disk with the `-` character:

```
cat wsprnet_spots.json | python3 wsprnet_azi_calc.py -i - -o - | head
```

### `ts_upload_batch.py`

```
ts_upload_batch.py [-h] -i [FILE] [-s FILE] [-a ADDRESS] [-d DATABASE] [-u USERNAME] [-p PASSWORD] [--log LOG]
```

`STDIN` can be used instead of an input file on disk with the `-` character:

```
cat wsprnet_spots.tsv | python3 ts_upload_batch.py -i -
```