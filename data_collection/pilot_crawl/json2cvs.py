import sys, os

import json
import csv


def load_json(filename):
	"""Returns content of a json file"""
	with open(filename, 'r') as fin:
		data = json.load(fin)
	return data


def write_to_csv(input_samples, output_file):
	"""Write a list of dictionaries to a csv file"""
	# Getting the filenames
	filenames = input_samples[0].keys()

	# Writing samples to csv
	f = csv.writer(open(output_file, 'w'))
	f.writerow(filenames)
	
	for sample in input_samples:
		f.writerow([sample[i] for i in filenames])



if __name__ == '__main__':

	json_input = sys.argv[1]

	# Getting the output path
	try:
		output_file = sys.argv[2]
	except:
		output_file = json_input.replace(".json", ".csv")

	sys.stdout.write("Loading doc: " + json_input +\
					"\nWriting to: " + output_file + "\n")

	# Loading the content, which must be a list of dictionaries
	json_content = load_json(json_input)

	if type(json_content) != list or type(json_content[0]) != dict:
		raise ValueError("Input content must be a list of dictionaries")

	write_to_csv(json_content, output_file)
