import pandas as pd
import os
import re

def split_text(input_file, output_dir):
    with open(input_file, 'r') as file:
        lines = file.readlines()

    thread_data = []
    current_thread = None

    for line in lines:
        if line.startswith("~~~~"):
            if current_thread:
                thread_data.append(current_thread)
            current_thread = {"Post": [], "Username": [], "PostsCounts": [], "JoinDate": [], "likedata": [], "textdata": []}
        elif current_thread is not None and line.strip():
            if line.startswith("Post:"):
                current_thread["Post"].append(line.strip().replace('Post: ', ''))
            elif line.startswith("Metadata:"):
                metadata = line.strip().replace('Metadata: ', '')
                username, posts_counts, join_date = split_metadata(metadata)
                current_thread["Username"].append(username)
                current_thread["PostsCounts"].append(posts_counts)
                current_thread["JoinDate"].append(join_date)
            elif line.startswith("likedata:"):
                current_thread["likedata"].append(line.strip().replace('likedata: ', ''))
            elif line.startswith("textdata:"):
                textdata = line.strip().replace('textdata: ', '').replace('|~~|~~|~~|', ' ')
                current_thread["textdata"].append(textdata)

    if current_thread:
        thread_data.append(current_thread)

    num_threads = len(thread_data)

    for i, thread in enumerate(thread_data):
        output_file = os.path.join(output_dir, f"thread_{i}.csv")
        process_thread(thread, output_file)

def split_metadata(metadata):
    parts = metadata.split('|~~|~~|~~|')
    username = parts[0].strip()
    posts_counts = ""
    join_date = ""

    for part in parts[1:]:
        if "Joined" in part:
            posts_counts, join_date = part.split("Joined", 1)
            posts_counts = posts_counts.strip()
            join_date = "Joined" + join_date.strip()

    return username, posts_counts, join_date

def process_thread(thread, output_file):
    df = pd.DataFrame(thread)
    df.to_csv(output_file, index=False)

if __name__ == "__main__":
    input_file = '/home/abdullah-zubair/WheelsMasail/Web Scraping/textPreprocessing/latestdata.txt'
    output_dir = '/home/abdullah-zubair/WheelsMasail/Web Scraping/textPreprocessing/PreProcessed_data'

    os.makedirs(output_dir, exist_ok=True)

    split_text(input_file, output_dir)
