import os, ffmpeg, json, time, datetime
from multiprocessing import Process
from colorama import Fore


# load config
with open("config.json") as f:
    config = json.load(f)
    IN_DIR = config["in_dir"]
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
    try:
        probe = ffmpeg.probe(out_file)
        out_duration = float(probe["format"]["duration"])
        diff = abs(duration - out_duration)
        if diff < 1:
            ratio = os.path.getsize(out_file) / os.path.getsize(in_file)
            msg(
                f"[{count}/{total}] Skipping {in_file}, Compression ratio {ratio * 100:.0f}%, diff {diff:.2f}s"
            )
            return
    except:
        pass
    # transcode
    start_time = time.perf_counter()
    msg(f"{Fore.GREEN}[{count}/{total}]Transcoding {in_file} {Fore.RESET}")
    # note that cuda decoding might have problem with h264
    try:
        ffmpeg.input(in_file).output(
            out_file, vcodec="av1_nvenc", cq=CONSTANT_QUALITY, acodec="copy"
        ).run(capture_stdout=True, capture_stderr=True, overwrite_output=True)
    except Exception as e:
        msg(f"{Fore.RED}Error transcoding {in_file}: {repr(e)}{Fore.RESET}")
        msg(f"{Fore.RED}Error message: {e.stderr.decode('utf-8')}{Fore.RESET}")  # type: ignore
        if os.path.exists(out_file):
            os.remove(out_file)
            msg(f"{Fore.RED}Removed {out_file}{Fore.RESET}")
        return
    end_time = time.perf_counter()
    speed = duration / (end_time - start_time)
    # check if the durations match
    probe = ffmpeg.probe(out_file)
    out_duration = float(probe["format"]["duration"])
    if abs(duration - out_duration) > 1:
        msg(f"{Fore.RED}Duration diff: {abs(duration - out_duration):.2f}s {in_file}{Fore.RESET}")
    else:
        msg(f"{Fore.GREEN}Duration diff: {abs(duration - out_duration):.2f}s {in_file}{Fore.RESET}")
    # check compression ratio
    ratio = os.path.getsize(out_file) / os.path.getsize(in_file)
    if ratio < 1:
        msg(f"{Fore.GREEN}Compressed {in_file} {ratio * 100:.0f}%, Speed {speed:.1f}X{Fore.RESET}")
    else:
        msg(f"{Fore.YELLOW}Compressed {in_file} {ratio * 100:.0f}%, Speed {speed:.1f}X{Fore.RESET}")
        os.remove(out_file)
        msg(f"{Fore.YELLOW}Removed {out_file}{Fore.RESET}")


def traverse(dir: str, args: list):
    out_dir = dir.replace(IN_DIR, OUT_DIR)
    for file in os.listdir(dir):
        if os.path.isdir(os.path.join(dir, file)):
            if not os.path.exists(os.path.join(out_dir, file)):
                os.mkdir(os.path.join(out_dir, file))
            traverse(os.path.join(dir, file), args)
            continue
        base_name, ext = os.path.splitext(file)
        if ext in [".flv", ".mp4", ".m4v"]:
            # filter by roomid
            roomid = base_name.split("_")[0]
            if not roomid in ROOMIDS:
                continue
            # get the full path of the file
            in_file = os.path.join(dir, file)
            # create the output file name
            out_file = os.path.join(out_dir, base_name + ".mp4")
            # add to the list of arguments
            args.append((in_file, out_file))


# loop over all files in the directory list
args = []
traverse(IN_DIR, args)

# sort by file name in ascending order
args.sort(key=lambda x: x[0])

# transcode in parallel
num_proc = 4
procs = []
total = len(args)
count = 0
while args:
    if len(procs) < num_proc:
        proc = Process(target=transcode, args=args.pop(0))
        count += 1
        proc.start()
        procs.append(proc)
    else:
        for i in range(len(procs)):
            if not procs[i].is_alive():
                procs[i].join()
                proc = Process(target=transcode, args=args.pop(0))
                count += 1
                proc.start()
                procs[i] = proc
                break
    time.sleep(1)
for proc in procs:
    proc.join()
msg("Done")
