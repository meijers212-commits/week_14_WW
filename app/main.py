import csv
from fastapi import FastAPI, File, UploadFile, HTTPException
import pandas as pd
from models import DataProcessing
from db import Dbinstractot
import uvicorn
from io import BytesIO, StringIO

app = FastAPI()


@app.post("/upload")
def upload_csv_file(file: UploadFile = File(...)):
    try:
        contents = file.file.read()
        buffer = BytesIO(contents)
        df = pd.read_csv(buffer)
        buffer.close()
        file.file.close()
        data = DataProcessing.data_processing_ful_tesk(df)
        conn = Dbinstractot.get_connection()
        Dbinstractot.creat_table(conn)
        Dbinstractot.insert_to_db(data)
        count = len(data)
        return {"status": "success", "inserted_records": count}
       

        
        

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", port=8081, host="localhost", reload=True)
