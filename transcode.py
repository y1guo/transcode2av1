import os, ffmpeg, json, time, datetime
from multiprocessing import Pool
from colorama import Fore, Style


# load config
with open("config.json") as f:
    config = json.load(f)
    IN_DIRS = config["in_dirs"]
    OUT_DIR = config["out_dir"]
    ROOMIDS = config["roomids"]
    CONSTANT_QUALITY = config["constant_quality"]


def msg(message: str):
    print(f"{datetime.datetime.now().strftime('%H:%M:%S')} {message}")


def transcode(in_file: str, out_file: str):
    assert in_file != out_file
    # get duration
    probe = ffmpeg.probe(in_file)
    duration = float(probe["format"]["duration"])
    # transcode
    start_time = time.perf_counter()
    msg(f"{Fore.GREEN}Transcoding {in_file}{Style.RESET_ALL}")
    ffmpeg.input(in_file, hwaccel="cuda", hwaccel_output_format="cuda").output(
        out_file, vcodec="av1_nvenc", cq=CONSTANT_QUALITY, acodec="copy"
    ).run(capture_stdout=True, capture_stderr=True, overwrite_output=True)
    end_time = time.perf_counter()
    speed = duration / (end_time - start_time)
    # check compression ratio
    in_size = os.path.getsize(in_file)
    out_size = os.path.getsize(out_file)
    ratio = out_size / in_size
    if ratio < 1:
        msg(
            f"{Fore.GREEN}Compressed {in_file} from {in_size} to {out_size} ({ratio * 100:.0f}%), Speed ({speed:.1f}X){Style.RESET_ALL}"
        )
    else:
        msg(
            f"{Fore.YELLOW}Compressed {in_file} from {in_size} to {out_size} ({ratio * 100:.0f}%), Speed ({speed:.1f}X){Style.RESET_ALL}"
        )
        os.remove(out_file)
        msg(f"{Fore.YELLOW}Removed {out_file}{Style.RESET_ALL}")


# loop over all files in the directory list
args = []
for dir in IN_DIRS:
    for file in os.listdir(dir):
        base_name, ext = os.path.splitext(file)
        if ext in [".flv", ".mp4", ".m4v"]:
            # filter by roomid
            roomid = base_name.split("_")[0]
            if not roomid in ROOMIDS:
                continue
            # get the full path of the file
            in_file = os.path.join(dir, file)
            # create the output file name
            out_file = os.path.join(OUT_DIR, base_name + ".mp4")
            # add to the list of arguments
            args.append((in_file, out_file))

# transcode in parallel
with Pool(4) as pool:
    pool.starmap(transcode, args)
