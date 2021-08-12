1.設定config
    "MongodbServerIP" : "127.0.0.1", //MongoDB ip
    "DBName":"Leapsy_clock_in_system", //資料庫名稱
    "Collection": "record-csv", //資料表名稱
    "StartDate" : "20201012",	//讀檔起始日
    "EndDate" : "20201106",	//讀檔結束日
    "FolderPath":"\\\\leapsy-nas3\\CheckInRecord\\"	目錄資料夾

2.csv檔案都在同一個資料夾(路徑FolderPath),全部檔案不用子目錄分類

3.csv檔名格式為 
ex:Rec20201012.csv

4.csv匯出格式:
打卡年月日,姓名,卡號,打卡時分,訊息,員工編號
ex:20201105,劉千華,2118239204,0731,10,053

5.每個欄位皆以逗號區隔(因此抓內容都是以逗號作為分界)

6.原本就是三碼 所以不額外處理員工編號的碼數問題


