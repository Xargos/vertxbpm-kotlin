import urllib.request
from multiprocessing import Pool

def f(x):
    print(urllib.request.urlopen("http://localhost:8080/workflow/SimpleWorkflow").read())

if __name__ == '__main__':
    while 1:
        with Pool(8) as p:
            p.map(f, list(range(0, 8)))

# print(f.read())