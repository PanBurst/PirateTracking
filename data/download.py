import os
import urllib.request
import zipfile

import tqdm


def download() -> None:
    print("Downloading data")
    url = "http://web.ais.dk/aisdata/aisdk-2023-05-01.zip"
    os.makedirs(r"data\dataset", exist_ok=True)
    filename = r"data\dataset\dataset.zip"

    with tqdm.tqdm(
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            miniters=1,
            desc="Downloading",
            total=0
    ) as progress_bar:
        def progress_hook(_, block_size, total_size):
            if progress_bar.total != total_size:
                progress_bar.total = total_size
            progress_bar.update(block_size)

        urllib.request.urlretrieve(url, filename, reporthook=progress_hook)


def extract() -> None:
    zip_filename = "data\dataset\dataset.zip"
    extract_folder = r"data\dataset"

    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall(extract_folder)
        old_file = os.path.join(extract_folder, "aisdk-2023-05-01.csv")
        new_file = os.path.join(extract_folder, "dataset.csv")
        os.rename(old_file, new_file)

    print("Zip file extracted successfully")


if __name__ == "__main__":
    if os.path.exists(r"data\dataset\dataset.zip"):
        print("Dataset is present")
    else:
        download()

    if os.path.exists(r"data\dataset\dataset.csv"):
        print("Dataset is present")
    else:
        extract()
