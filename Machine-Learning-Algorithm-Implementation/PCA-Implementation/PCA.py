from mlxtend.data import loadlocal_mnist
import matplotlib.pyplot as plt
import numpy as np
from sklearn.decomposition import PCA

LABEL_FILE = "emnist-letters-train-labels-idx1-ubyte";
IMAGE_FILE = "emnist-letters-train-images-idx3-ubyte";

X, y = loadlocal_mnist(images_path=IMAGE_FILE,labels_path=LABEL_FILE)
pca = PCA(n_components=25)
pca.fit(X)
P = pca.fit_transform(X)

with open("minst-train", "w") as minst:
    for s in range(P.shape[0]):
        FeatureLabel = y[s].tolist()
        FeatureList = P[s].tolist()
        minst.write(str(FeatureLabel) + ": " + ' '.join(str(d) for d in FeatureList) + "\n")
print("Write " + str(P.shape[0]) + " Line")

LABEL_FILE = "emnist-letters-test-labels-idx1-ubyte";
IMAGE_FILE = "emnist-letters-test-images-idx3-ubyte";

X, y = loadlocal_mnist(images_path=IMAGE_FILE,labels_path=LABEL_FILE)
pca = PCA(n_components=25)
pca.fit(X)
P = pca.fit_transform(X)

with open("minst-test", "w") as minst:
    for s in range(P.shape[0]):
        FeatureLabel = y[s].tolist()
        FeatureList = P[s].tolist()
        minst.write(str(FeatureLabel) + ": " + ' '.join(str(d) for d in FeatureList) + "\n")
print("Write " + str(P.shape[0]) + " Line")
