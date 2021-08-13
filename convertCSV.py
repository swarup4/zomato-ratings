import pandas as pd
import numpy as np
  
def convert(filename):
    name = filename.split('.')[0]
    # Reading the csv file
    df = pd.read_csv(filename)
    
    # saving xlsx file
    write = pd.ExcelWriter(name + '.xlsx')
    df.to_excel(write, index = False)
    
    write.save()


convert('zomato.csv')