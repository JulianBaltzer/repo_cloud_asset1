from multiprocessing.sharedctypes import Value


eingabe = input("alter eingeben")

try:
    #if(int(eingabe) > 150):
       # raise ValueError
    print("toll")
except ValueError:
    print("gdfgdfgdgf")
    
except TypeError:
    print("typeerror")