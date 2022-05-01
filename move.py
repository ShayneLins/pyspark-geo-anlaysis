from scipy.sparse import lil_matrix, csr_matrix
from numpy import random as nran
import numpy as np
import cython
cimport numpy as np

np.import_array()

def MultiNom(X: np.ndarray, D: csr_matrix):
    place:tuple = np.nonzero(X)[0]
    X1: np.ndarray
    if len(place)==0:
        X1=X
    else:
        D=D.transpose()
        tmpD:lil_matrix = lil_matrix(D)
        Drow:np.ndarray = tmpD.rows
        Ddata:np.ndarray = tmpD.data
        #start_time=time.time()

        move=np.zeros((5080))
        for k in place:
            nk=X[k]
            y=Ddata[k]
            
            if (len(y)>1):
                z = nran.multinomial(n=nk, pvals=y)
                move[Drow[k]]=move[Drow[k]]+z
            
            elif (len(y)==1):
                move[Drow[k]]=move[Drow[k]]+nk
                
            else:
                move[k]=move[k]+nk
                
        X1=move
    return X1