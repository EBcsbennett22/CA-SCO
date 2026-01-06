import cv2
import shutil
from pathlib import Path
from PIL import Image
import pillow_heif

# Enable HEIC support
pillow_heif.register_heif_opener()

# ---------------- CONFIG ----------------
ORIGIN_DIR = Path("C:\\Users\\cb1152\\Downloads\\Photo2")     # input folder
TARGET_DIR = Path("C:\\Users\\cb1152\\Downloads\\Cleaned_Photos2")   # output folder
TARGET_DIR.mkdir(exist_ok=True)
# ----------------------------------------

IMAGE_EXTS = {".jpg", ".jpeg", ".png"}
HEIC_EXTS = {".heic"}
VIDEO_EXTS = {".mp4", ".mov"}

def save_as_jpg(img: Image.Image, dst: Path):
    img.convert("RGB").save(dst, "JPEG", quality=95)

def process_image(src: Path, dst: Path):
    img = Image.open(src)
    save_as_jpg(img, dst)

def process_heic(src: Path, dst: Path):
    img = Image.open(src)
    save_as_jpg(img, dst)

def process_video(src: Path, dst: Path):
    cap = cv2.VideoCapture(str(src))
    cap.set(cv2.CAP_PROP_POS_MSEC, 500)  # grab frame at 0.5s
    success, frame = cap.read()
    cap.release()

    if not success:
        raise RuntimeError("Could not extract frame")

    cv2.imwrite(str(dst), frame)

def process_file(file_path: Path):
    ext = file_path.suffix.lower()
    output_path = TARGET_DIR / f"{file_path.stem}.jpg"

    if ext in IMAGE_EXTS:
        print(f"Image → JPG: {file_path.name}")
        process_image(file_path, output_path)

    elif ext in HEIC_EXTS:
        print(f"HEIC → JPG: {file_path.name}")
        process_heic(file_path, output_path)

    elif ext in VIDEO_EXTS:
        print(f"Video → JPG: {file_path.name}")
        process_video(file_path, output_path)

    else:
        print(f"Skipping unsupported: {file_path.name}")

def main():
    for file in ORIGIN_DIR.iterdir():
        if file.is_file():
            try:
                process_file(file)
            except Exception as e:
                print(f"❌ Failed {file.name}: {e}")

    print("✅ All files processed.")

if __name__ == "__main__":
    main()

