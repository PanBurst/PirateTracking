import os
import zipfile
import tqdm

import urllib.request

def downloadDatasetZip() -> None:
    print("Downloading data")
    url = "http://web.ais.dk/aisdata/aisdk-2023-05-01.zip"
    os.makedirs(r"data\dataset", exist_ok=True)
    filename = r"data\dataset\dataset.zip"
    with tqdm.tqdm(unit='B', unit_scale=True, unit_divisor=1024, miniters=1, desc="Downloading", total=0) as progress_bar:
        def progress_hook(count, block_size, total_size):
            if progress_bar.total != total_size:
                progress_bar.total = total_size
            progress_bar.update(block_size)

        urllib.request.urlretrieve(url, filename, reporthook=progress_hook)

def extractDatasetZip() -> None:
    zip_filename = "data\dataset\dataset.zip"
    extract_folder = r"data\dataset"

    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall(extract_folder)
        old_file = os.path.join(extract_folder, "aisdk-2023-05-01.csv")
        new_file = os.path.join(extract_folder, "dataset.csv")
        os.rename(old_file, new_file)
    
    print("Zip file extracted successfully")

def checkDatasetZip() -> bool:
    if os.path.exists(r"data\dataset\dataset.zip"):
        extractDatasetZip()
        return True
    else:
        return False
    
def checkDatasetFolder() -> bool:
    
    if os.path.exists(r"data\dataset\dataset.csv"):
        return True
    else:
        return False

if __name__ == "__main__":
    if(checkDatasetZip()):
        print("Dataset is present")
    else:
        downloadDatasetZip()
    if(not checkDatasetFolder()):
        extractDatasetZip()
    else:
        print("Dataset is present")




    
