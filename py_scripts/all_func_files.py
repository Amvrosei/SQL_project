'''
функция берет файлы из папки files с именами:
passport_blacklist*
transactions*
terminals*
'''

import os
import sys
import shutil


# Получаем список файлов по фильтрам

def get_files(dir_path):

    passport_blacklist = []
    transactions = []
    terminals = []

    for dirpath,_,filenames in os.walk(dir_path):
        for f in filenames:
           # print(os.path.realpath(f))
            if f.__contains__('passport_blacklist'):
                passport_blacklist.append([f,os.path.abspath(os.path.join(dirpath, f))])
            elif f.__contains__('transactions'):
                transactions.append([f,os.path.abspath(os.path.join(dirpath, f))])
            elif f.__contains__('terminals'):
                terminals.append([f,os.path.abspath(os.path.join(dirpath, f))])
    hash_tab = {}
    hash_tab['passport_blacklist'] = passport_blacklist
    hash_tab['transactions'] = transactions
    hash_tab['terminals'] = terminals
    
    return hash_tab

#print(get_files(dir_path))

# копирует и удаляет файлы
# на вход полный путь 
# copy_file_and_remove(r'C:\\Users\\andy\\Documents\\DE_SQL\\FinalProject\\files\\test.txt')

def copy_file_and_remove(file_path):
    file_name = os.path.basename(file_path)
    current_folder = os.path.dirname(file_path) #os.path.basename(file_path)
    target_path = os.path.join(current_folder.replace('files','archive'), file_name + ".backup")

    try:
        shutil.copy(file_path, target_path)
        os.remove(file_path)
    except Exception as ex:
        print(ex)   


        
# def copy_file_and_remove(file_path):
#     base_name = os.path.basename(file_path)
#     target_path = os.path.join('archive', base_name+".backup")
#     try:
#         shutil.copy(file_path, target_path)
#         os.remove(file_path)
#     except Exception as Ex:
#         print(Ex)   
# copy_file_and_remove(r'C:\\Users\\andy\\Documents\\DE_SQL\\FinalProject\\files\\test.txt')
        
        

# def get_files(dir_path):

#     passport_blacklist = []
#     transactions = []
#     terminals = []

#     for f in os.listdir(dir_path):
#         print(os.path.realpath(f))
#         if f.__contains__('passport_blacklist'):
#             passport_blacklist.append([f,os.path.realpath(f)])
#         elif f.__contains__('transactions'):
#             transactions.append([f,os.path.realpath(f)])
#         elif f.__contains__('terminals'):
#             terminals.append([f,os.path.realpath(f)])
#     hash_tab = {}
#     hash_tab['passport_blacklist'] = passport_blacklist
#     hash_tab['transactions'] = transactions
#     hash_tab['terminals'] = terminals
    
#     return hash_tab

#print(get_files(dir_path))