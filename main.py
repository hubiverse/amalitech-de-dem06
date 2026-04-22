import kagglehub

def main():

    # Download latest version
    path = kagglehub.dataset_download("mahatiratusher/flight-price-dataset-of-bangladesh")

    print("Path to dataset files:", path)
    print("Hello from dem06!")


if __name__ == "__main__":
    main()
