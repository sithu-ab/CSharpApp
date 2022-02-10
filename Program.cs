using System.Data;
using System.Globalization;
using ChoETL;
using Deedle;
using pd = PandasNet;

namespace CSharpApp
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello C#!");

            var input_file = "./input/感+プ_07月_千葉県_異常除去済_2_534040_233.csv"; // 入力ファイル

            // ReadCsvIntoDeedleDataFrame(input_file);

            // 1.
            DataTable tbl = ReadCSV(input_file);
            pd.DataFrame df = CreateDataFrameFromDataTable(tbl);
            Console.WriteLine(df);

            // 2.
            // GroupBy(df);
        }

        /**
         * Read CSV content using ChoELT library
         */
        static DataTable ReadCSV(string input_file)
        {
            Console.WriteLine("-----");
            Console.WriteLine(input_file);

            var p = new ChoCSVReader(input_file).WithFirstLineHeader();
            var table = p.AsDataTable();

            // Console.WriteLine(df.Columns[0]);

            // foreach(DataColumn column in df.Columns)
            // {
            //     Console.WriteLine(column.ColumnName);
            //     Console.WriteLine(column.DataType);
            //     if (column.ColumnName == "旅行時間情報") {
            //         Console.WriteLine("旅行時間情報");
            //     }
            // }

            return table;
        }

        /**
         * Create Pandas DataFrame from DataTable
         */
        static pd.DataFrame CreateDataFrameFromDataTable(DataTable tbl)
        {
            var rows = new List<PandasNet.Series>();

            foreach(DataRow row in tbl.Rows) {
                rows.Add(new pd.Series(row.ItemArray.Select(p => p.ToString()).ToArray()));
            }

            return new pd.DataFrame(rows);
        }

        /**
         * Group by data from DataTable using IDictionary
         */
        static IDictionary<string, List<object>> GroupBy(DataTable tbl)
        {
            Console.WriteLine("-----");

            IDictionary<string, List<object>> dict = new Dictionary<string, List<object>>();


            foreach(DataRow row in tbl.Rows) {
                var s = new pd.Series(row.ItemArray);

                // [データの前処理]ID01旅行時間の”8191”をnullに置換(該当行は消さない)
                // df.loc[df["旅行時間情報"] == 8191, "旅行時間情報"] = np.nan
                // 旅行時間情報 = row[4]
                row[4] = Convert.ToInt32(row[4]) == 8191 ? null : Convert.ToInt32(row[4]);
                Console.WriteLine(row[4]);
                Console.WriteLine(row[0]);

                // 日付追加
                // df["日付"] = pd.to_datetime(df["送信時刻"]).dt.date
                // 送信時刻 = row[0]
                DateTime transmissionTime = DateTime.ParseExact(Convert.ToString(row[0]),  "yyyy/M/d H:mm", CultureInfo.InvariantCulture); // df["日付"] = pd.to_datetime(df["送信時刻"]).dt.date
                row[0] = transmissionTime;

                var key = row[1] + "-" + transmissionTime.Year + transmissionTime.Month + transmissionTime.Day;
                if (!dict.ContainsKey(key)) {
                    dict[key] = new List<object>();
                }

                dict[key].Add(row);

                Console.WriteLine(row[0]);
            }

            Console.WriteLine("Aggregate: " + dict.Count);

            foreach(var item in dict) {
                Console.WriteLine("group: " + item.Key);
                Console.WriteLine("df_g: " + item.Value.Count);
            }

            return dict;
        }

        /**
         * Read CSV into DataFrame using Deedle library
         */
        static void ReadCsvIntoDeedleDataFrame(string input_file)
        {
            var df = Frame.ReadCsv(input_file);
            Console.WriteLine(df);

            Console.WriteLine(df.ColumnCount);
            Console.WriteLine(df.Columns);
        }
    }
}
