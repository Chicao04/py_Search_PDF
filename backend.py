from flask import Flask, jsonify, request
import mysql.connector

app = Flask(__name__)

# Kết nối đến MySQL
connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='',
    database='your_database'
)

