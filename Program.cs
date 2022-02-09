using System.Data;
using ChoETL;

namespace CSharpApp
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello C#!");

            CsvReader();
        }

        static void CsvReader()
        {
            var input_file = "./input/感+プ_07月_千葉県_異常除去済_2_534040_233.csv"; // 入力ファイル
            Console.WriteLine(input_file);

            var p = new ChoCSVReader(input_file).WithFirstLineHeader();
            var df = p.AsDataTable();

            Console.WriteLine(df.Columns[0]);
            Console.WriteLine("-----");

            // foreach(DataColumn column in df.Columns)
            // {
            //     Console.WriteLine(column.ColumnName);
            //     Console.WriteLine(column.DataType);
            //     if (column.ColumnName == "旅行時間情報") {
            //         Console.WriteLine("旅行時間情報");
            //     }
            // }

            Console.WriteLine("-----");

            foreach(DataRow row in df.Rows) {
                // [データの前処理]ID01旅行時間の”8191”をnullに置換(該当行は消さない)
                // df.loc[df["旅行時間情報"] == 8191, "旅行時間情報"] = np.nan
                // 旅行時間情報 = row[4]
                row[4] = Convert.ToInt32(row[4]) == 8191 ? null : Convert.ToInt32(row[4]);
                Console.WriteLine(row[4]);
                Console.WriteLine(row[0]);

                // 日付追加
                // df["日付"] = pd.to_datetime(df["送信時刻"]).dt.date
                // 送信時刻 = row[0]
                DateTime dt = DateTime.ParseExact(Convert.ToString(row[0]),  "yyyy/M/d H:mm", System.Globalization.CultureInfo.InvariantCulture);
                row[0] = dt;
                Console.WriteLine(row[0]);
            }
        }
    }
}
