#https://www.youtube.com/watch?v=xpPX3fBM9dU
# not finished

import numpy as np
import pandas as pd

rg = np.random.default_rng()


def generate_random_data(n_features, n_values):
  features = rg.random((n_features,n_values))
  weights = rg.random((1, n_values))[0]
  targets = np.random.choice([0,1], n_features)
  data = pd.DataFrame(features, columns = ["x1", "x2", "x3"])
  data["targets"] = targets
  return data, weights

def get_weighted_sum(feature, weights, bias):
  return np.dot(feature, weights) + bias

def sigmoid(w_sum):
  return 1/(1+np.exp(-w_sum))


data, weights = generate_random_data(4, 3)
print(data)

bias = 0.1
data = data.loc[:, data.columns!='targets']

w_sum = get_weighted_sum(data, weights, bias)

prediction = sigmoid(w_sum)
print(prediction)
