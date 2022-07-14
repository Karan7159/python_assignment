gitimport argparse
import pandas as pd
import json
import os


def get_params() -> dict:
	parser = argparse.ArgumentParser(description='DataTest')
	parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
	parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
	parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
	parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
	return vars(parser.parse_args())

def read_data():

	try:
		cust = pd.read_csv('..\\input_data\\starter\\customers.csv')
	except:
		print("Error reading customers file")
		exit()

	try:
		prod = pd.read_csv('..\\input_data\\starter\\products.csv')
	except:
		print("Error reading products file")
		exit()

	return [cust, prod]

def read_transactions():
	rootdir='..\\input_data\\starter\\transactions'
	transactions=list()
	for file in os.listdir(rootdir):												#List all the contents of rootdir
		d = os.path.join(rootdir, file)												#Make entire path name to check if it is directory in next line
		if os.path.isdir(d):                            							#Since all transactions are in directories, filtering out any other files that may exist in that directory
			d = os.path.join(d, 'transactions.json')								#Assuming other files in transactions dir are handled in a different way. We can filter out other files in 
			with open(d) as f:														#a separate list
				for jsonObj in f:
					t = json.loads(jsonObj)
					transactions.append(t)

	return transactions

def get_cust_id(transactions, i, cust):
	cust_id=transactions[i]['customer_id']											#Take the customer id

	c=cust.loc[cust['customer_id'] == cust_id]										#Search for that customer ID in customers.csv dataframe(cust)
	if c.empty:																		#Checking if customer ID exists. If it doesn't, 'c' dataframe will be empty 
		print('empty df')
		exit()

	cl=c.values.tolist()[0]															#Gives customer_id, loyalty score
	#c is the matched row from customers.csv dataframe 
	#c.values converts c from a Pandas Series to a Numpy array 
	# tolist() converts c from a Numpy array to a Python List 						# This step might be redundant. Maybe there's a better way to get cust_id and loyalty_score
	# [0] indexing is just to unpack the 2D list obtained from above operations
	return cl


def get_prod_details(basket, j, prod):
	prod_id=basket[j]['product_id']													#Get each item's product ID
	price=basket[j]['price']														#Get price for corresponding item

	p=prod.loc[prod['product_id'] == prod_id]										#Search for product ID in products.csv dataframe
	if p.empty:																		#Checking if product ID exists. If it doesn't, p dataframe will be empty 
		print('empty df')
		exit()

	pl=p.values.tolist()[0]															#Gives product_id, product_description, product_category
	pl=[pl[0], pl[2]]																#Since we only need product_id and product_category
	pl.append(price)																#Assuming purchase_count is the price
	return pl

def add_to_database(cl, pl, database):
	cl2=cl.copy()																	#Copy to not disturb the matched [customer_id, loyalty_score] obtained in 'cl'
	cl2.extend(pl)																	#Making one entire row of our required result (['customer_id', 'loyalty_score', 'product_id', 'product_category', 'purchase_count'])
	database = database.append(pd.DataFrame([cl2], columns=['customer_id', 'loyalty_score', 'product_id', 'product_category', 'purchase_count']), ignore_index=True)
	return database


def main():
	params = get_params()

	cust, prod=read_data()
	transactions=read_transactions()
	# print(transactions[0])														#Sample entry in transactions list
	# {
	#	'customer_id': 'C20', 
	#	'basket': [{'product_id': 'P27', 'price': 29}, {'product_id': 'P34', 'price': 1174}, {'product_id': 'P28', 'price': 1541}],
	#	'date_of_purchase': '2018-12-01 21:01:00'
	# }

	database = pd.DataFrame(columns=['customer_id', 'loyalty_score', 'product_id', 'product_category', 'purchase_count'])		#Made empty dataframe for required result 
	
	transactions_length=len(transactions)
	for i in range(0,transactions_length):											#For all transactions by a customer
		cl=get_cust_id(transactions, i, cust)
		
		basket=transactions[i]['basket']											#Get customer's basket
		basket_count=len(basket)
		for j in range(0, basket_count):											#For all items in the basket
			pl=get_prod_details(basket, j, prod)
			database=add_to_database(cl, pl, database)

	print(database)


if __name__ == "__main__":
	main()
