import s3fs

fs = s3fs.S3FileSystem(
    key="test",
    secret="test",
    client_kwargs={"endpoint_url": "http://localhost:4566"}
)

print("Buckets:", fs.ls(""))
print("Retail gold:", fs.ls("retail-gold"))
print("Revenue KPIs:", fs.ls("retail-gold/revenue_kpis"))