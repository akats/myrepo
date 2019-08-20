#!/usr/bin/env python3
import argparse
import csv
import json
import os
import os.path as osp
import pandas as pd
import uuid

# 0.  Parse arguments
parser=argparse.ArgumentParser()
parser.add_argument("--name")
parser.add_argument("--user_uuid")

args=parser.parse_args()
ingredient_name=args.name
if not ingredient_name:
	print("Ingredient name not specified")
	exit(-1)
user_uuid=None
if args.user_uuid:
	user_uuid = args.user_uuid
# print("searching for ingredient ", ingredient_name)

# 1.  Load files into memory.  In production, this can be done once for many requests
products_file = "Products.csv"
nutrients_file = "Nutrients.csv"
products_df = pd.read_csv(products_file)
products_df.set_index("NDB_Number", inplace=True)
nutrients_df = pd.read_csv(nutrients_file)
nutrients_df.set_index(["NDB_No", "Nutrient_Code"], inplace=True)

# 2.  look up the indices of the products that match
sought_products = products_df[products_df["long_name"].str.contains(ingredient_name.upper())]

res = []
for product_idx, product in sought_products.iterrows():
	product_name = product["long_name"]
	#product_idx = product["NDB_Number"]
	try:
		protein_amount = nutrients_df.loc[(product_idx, 203)]["Output_value"]
	except (KeyError, TypeError) as e:
		protein_amount = None
	# not a great design because DRY, but we'll do for now
	try:
		fat_amount = nutrients_df.loc[(product_idx, 204)]["Output_value"]
	except (KeyError, TypeError) as e:
		fat_amount = None
	try:
		carbohydrate_amount = nutrients_df.loc[(product_idx, 205)]["Output_value"]
	except (KeyError, TypeError) as e:
		carbohydrate_amount = None

	single_product_dict = {"product_name":product_name,
						   "Fats": fat_amount, "Proteins": protein_amount, "Carbs": carbohydrate_amount}
	res.append(single_product_dict)

#print("res", res)
# 3 save results
out_filename = str(uuid.uuid4()) + ".json"
# if we are creating user models from their search terms, we need to keep all the queries issued by an individual user together.
# Therefore, we create a username folder and put all the searches there.  Each search has to be in a separate filename
# because ultimately this ends up in S3, and S3 does not allow file append operations.  We'll probably go with some kind of
# an ephemeral container architecture, so we'll have to put the results in S3 right away.
# While having each search be a separate file will add to our processing time, at least we are saving a map-reduce step because
# we don't need to group by user.  If we create multiple models, we can create a data processing pipeline that takes these
# raw files and converts them to a columnar format that's faster to access.
if user_uuid:
	os.makedirs(user_uuid, exist_ok=True)
	out_filename = osp.join(user_uuid, out_filename)

with open(out_filename, 'w') as f:
	json.dump(res, f, indent=0, sort_keys=True)
	print("recorded results to " + out_filename)
