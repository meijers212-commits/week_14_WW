from fastapi import FastAPI, File, UploadFile, HTTPException
from app.models import DataProcessing
from app.db import Dbinstractot

app = FastAPI()


@app.post("/upload")
def upload_csv_file(file: UploadFile = File(...)):
    try:
        data = DataProcessing.data_processing_ful_tesk(file=file)
        Dbinstractot.creat_table()
        Dbinstractot.insert_to_db(data=data)
        count = len(data)
        return {"status": "success", "inserted_records": str(count)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", port=5000, host="localhost", reload=True)
