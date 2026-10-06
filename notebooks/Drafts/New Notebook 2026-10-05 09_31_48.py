# Databricks notebook source
import urllib.request
try:
    ip = urllib.request.urlopen("https://checkip.amazonaws.com", timeout=10).read().decode().strip()
    print("Internet OK, serverless egress IP:", ip)
except Exception as e:
    print("No internet access:", e)