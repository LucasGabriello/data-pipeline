import os
import requests
import pandas as pd
from create_csv import create_csv as cc

ep_products = "https://fakestoreapi.com/products"
ep_categories = "https://fakestoreapi.com/products/categories"
ep_carts = "https://fakestoreapi.com/carts"
ep_users = "https://fakestoreapi.com/users"

cc(ep_products,"products")
cc(ep_categories,"categories")
cc(ep_carts,"carts")
cc(ep_users,"users")
