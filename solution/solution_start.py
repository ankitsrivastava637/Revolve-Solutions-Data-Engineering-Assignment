import argparse
import os
import pandas as pd
import json
from itertools import groupby


def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    return vars(parser.parse_args())




# transactions() function reads all records of all transaction.json files for all records,
# and normalises the json data of column 'basket'
# and merges into single dataframe
def transcations():
    params = get_params()
    transaction_path = params['transactions_location']

    path = transaction_path
    c = 0
    column_names = ['product_id','price', 'customer_id', 'date_of_purchase']   
    df = pd.DataFrame(columns=column_names)
    for root,d_names,f_names in os.walk(path):
        c += 1
        ls = []
        if c>1 :
            with open(root+"/transactions.json", 'r') as myfile:
                for jsonObj in myfile:
                    data = json.loads(jsonObj)
                    ls.append(data)
                
                df_nested_list = pd.json_normalize(ls,  record_path =['basket'],meta=['customer_id', 'date_of_purchase'])
                df = pd.concat([df,df_nested_list],axis=0)
        
    return df




def read_data_frame(path):
    data_df = pd.read_csv(path)
    return data_df
    
            

def joining_data_frames(transactions_data,customer_data,product_data):
    trans_prod_data = pd.merge(transactions_data, 
                      product_data, 
                      on ='product_id', 
                      how ='inner')
    final_data =  pd.merge(trans_prod_data, 
                    customer_data, 
                    on ='customer_id', 
                    how ='inner')

    num_of_items_purchase = final_data.groupby(['customer_id']).size().to_frame()
    num_of_items_purchase.rename( columns={0:'total_num_of_products_purchased_by_customer'}, inplace=True )    
    num_of_purchases = final_data.groupby(['customer_id'])['date_of_purchase'].nunique().to_frame()       
    num_of_purchases.rename(columns={'date_of_purchase' : "customer's_purchase_total_times"}, inplace=True )
    

    final_data =  pd.merge(final_data, 
                    num_of_items_purchase, 
                    on ='customer_id', 
                    how ='inner')
    final_data =  pd.merge(final_data, 
                    num_of_purchases, 
                    on ='customer_id', 
                    how ='inner')
    final_json = list(final_data.T.to_dict().values()) 
    return final_json  
    




def processed_data(data):
    
    #for getting customer record (without repeating values inside 'product_id' )
    res1 = []
    key_func = lambda k: k['customer_id']

    for k, g in groupby(sorted(data, key=key_func), key=key_func):
        obj1 = { 'customer_id': k, 'customer_id': '', 'product_id': [], 'product_category': []}
        product_id_count = 0
        for group in g:
            if not obj1['customer_id']:
                obj1['customer_id'] = group['customer_id']
            
            if group['product_id'] not in obj1['product_id']:   
                obj1['product_id'].append(group["product_id"])
                obj1['product_category'].append(group['product_category'])

        obj1['total_num_of_products_purchased'] = group['total_num_of_products_purchased_by_customer']
        obj1['loyalty_score'] = group['loyalty_score']
        obj1['purcase_count'] = group["customer's_purchase_total_times"]
        obj1['total_num_items_purchase'] = group['total_num_of_products_purchased_by_customer']
        res1.append(obj1)


    #for getting customer record (repeating values inside 'product_id' based on purchase on different dates) 
    res = []
    key_func = lambda k: k['customer_id']

    for k, g in groupby(sorted(data, key=key_func), key=key_func):
        obj = { 'customer_id': k, 'customer_id': '', 'product_id': [], 'product_category': []}
        product_id_count = 0
        for group in g:
            if not obj['customer_id']:
                obj['customer_id'] = group['customer_id']
              
            obj['product_id'].append(group["product_id"])
            obj['product_category'].append(group['product_category'])
                
        obj['loyalty_score'] = group['loyalty_score']
        obj['purcase_count'] = group["customer's_purchase_total_times"]
        obj['total_num_items_purchase'] = group['total_num_of_products_purchased_by_customer']
        res.append(obj)

    ob = []

    #convert customer record with desired columns in desired format 
    for i in range(len(res)):

        #count of values in 'product_id' in each row of record 'res'
        counts = pd.Series(res[i]['product_id']).value_counts().to_dict()

        y = {'products_id_counts': counts}
        z = {'product_id' : res1[i]['product_id']}
        w = {'product_category': res1[i]['product_category']}
      
        #update values of specified category of each row of 'res' with values of sparticular category of each row of 'res1'
        #gets desired format data
        res[i].update(y)
        res[i].update(z)
        res[i].update(w)

        ob.append(res[i])

    print(res)

    #write final customer record data inside Output_data.json file.
    jsonFile = open("Output_data.json", "w")
    jsonFile.write(str(res))
    jsonFile.close()




#Categories in Output_data.json:
#purcase_count : number of times customer did shopping
#total_num_items_purchase : total number of products purchased
#products_id_counts : contains each product_id as key and it's corresponding number of purchase by partcular customer as value
    


                 
def main():

    #returns dataframe 
    transactions_data = transcations()

    params = get_params()
    customer_path = params['customers_location']

    #reads customer.csv, returns dataframe
    customer_data = read_data_frame(customer_path)

    products_path = params['products_location']

    #reads products.csv , returns dataframe
    product_data = read_data_frame(products_path)

    #gets merged json data of all 3 , transactions,customer_data, product_data 
    json_data = joining_data_frames(transactions_data,customer_data,product_data)

    #gets the customer record with desired category values inside final Output_data.json
    processed_data(json_data)


      
        

if __name__ == "__main__":
    main()
