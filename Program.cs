using System.Data;
using System.Globalization;
using ChoETL;
using Deedle;
using pd = PandasNet;

namespace CSharpApp
{
    class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Hello C#!");

            var input_file = "./input/感+プ_07月_千葉県_異常除去済_2_534040_233.csv"; // 入力ファイル

            var tbl = ReadCsv(input_file);
            var (group, df) = GroupBy(tbl);

            Console.WriteLine(group);
            Console.WriteLine(df);

            foreach (KeyValuePair<string, List<object>> gp in group)
            {
                Console.WriteLine(gp.Key);
                var x = MinMaxScale(df[gp.Key][4]);
                Console.WriteLine(x[0]);
            }

            // ReadCsvIntoDeedleDataFrame(input_file);

            // pd.DataFrame df = CreateDataFrameFromDataTable(tbl);
            // pd.DataFrame(df[4].ConvertToEnumerable().Select(p => Convert.ToInt32(p) == 235 ? 0 : Convert.ToInt32(p)));
            // Console.WriteLine(df[4]);

            // var p = new pd.Pandas();
            // pd.DataFrame df = p.read_csv(input_file);
            // Console.WriteLine(df);

            // var s = p.to_datetime(df[0].index);
            // Console.WriteLine(df[1]);
        }

        /**
         * Read CSV content using ChoELT library
         */
        private static DataTable ReadCsv(string input_file)
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
         * Create custom Series
         */
        private static List<List<object>> CreateEmptySeries(int columnCount)
        {
            var customFrame = new List<List<object>>();
            for (var c = 0; c < columnCount; c++)
            {
                customFrame.Insert(c, new List<object>());
            }

            return customFrame;
        }
        /**
         * Group by data from DataTable using IDictionary
         */
        private static (IDictionary<string, List<object>>, IDictionary<string, List<List<object>>>) GroupBy(DataTable tbl)
        {
            IDictionary<string, List<object>> dict = new Dictionary<string, List<object>>();
            IDictionary<string, List<List<object>>> frames = new Dictionary<string, List<List<object>>>();
            // var series = new List<pd.Series>();
            var customFrame = CreateEmptySeries(tbl.Columns.Count);
            int? fFillValue = null;
            int? bFillValue = null;
            string? key = null;

            Console.WriteLine("-----");

            var i = 0;
            foreach (DataRow row in tbl.Rows)
            {
                // [データの前処理]ID01旅行時間の”8191”をnullに置換(該当行は消さない)
                // 旅行時間情報 = row[4]
                row[4] = Convert.ToInt32(row[4]) == 8191 ? null : Convert.ToInt32(row[4]); // df.loc[df["旅行時間情報"] == 8191, "旅行時間情報"] = np.nan
                if (i == 1)
                {
                    bFillValue = !row[4].IsNullOrDbNull() ? Convert.ToInt32(row[4]) : null;
                }

                i++;
            }

            i = 0;
            foreach (DataRow row in tbl.Rows)
            {
                if (row[4].IsNullOrDbNull())
                {
                    // 欠損値を、欠損値の次にある値で補完する
                    // df_g["旅行時間情報"] = df_g["旅行時間情報"].fillna(method='bfill')
                    // df_g["旅行時間情報"] = df_g["旅行時間情報"].fillna(method='ffill')
                    row[4] = i == 0 ? bFillValue : fFillValue;
                }
                else
                {
                    row[4] = Convert.ToInt32(row[4]);
                }

                fFillValue = !row[4].IsNullOrDbNull() ? Convert.ToInt32(row[4]) : null;

                // 日付追加
                // 送信時刻 = row[0]
                DateTime transmissionTime = DateTime.ParseExact(Convert.ToString(row[0]), "yyyy/M/d H:mm",
                    CultureInfo.InvariantCulture); // df["日付"] = pd.to_datetime(df["送信時刻"]).dt.date
                row[0] = transmissionTime;

                // Grouping by column 1 and 0
                key = row[1] + "-" + transmissionTime.Year + transmissionTime.Month + transmissionTime.Day;
                if (!dict.ContainsKey(key))
                {
                    dict[key] = new List<object>();
                    if (i > 0)
                    {
                        frames.Add(key, customFrame);
                        customFrame = CreateEmptySeries(tbl.Columns.Count);
                    }

                    // Console.WriteLine(series.Count);
                    // if (series.Count > 0)
                    // {
                    //     frames.Add(key, new pd.DataFrame(series));
                    //     series = new List<pd.Series>();
                    // }
                }

                dict[key].Add(row);
                // series.Add(new pd.Series(row.ItemArray.Select(p => p.ToString()).ToArray()));

                // Push to custom series
                for (var c = 0; c < tbl.Columns.Count; c++)
                {
                    // if (c == 4)
                    // {   // 4th column: 旅行時間情報
                    //     row[c] = Convert.ToInt32(row[c]);
                    // }
                    customFrame[c].Add(row[c]);
                }

                i++;
            }

            if (!key.IsNull())
            {
                frames.Add(key, customFrame);
            }
            // if (!key.IsNull() && series.Count > 0)
            // {
            //     frames.Add(key, new pd.DataFrame(series));
            // }

            Console.WriteLine("Aggregate: " + dict.Count);
            Console.WriteLine("Frames: " + frames.Count);

            foreach (var item in dict)
            {
                Console.WriteLine("group: " + item.Key);
                Console.WriteLine("df_g: " + item.Value.Count);
            }

            return (dict, frames);
        }

        private static List<double> MinMaxScale(List<object> x, int from = 0, int to = 1)
        {
            var min = x.Select(p => Convert.ToInt32(p)).Min();
            var max = x.Select(p => Convert.ToInt32(p)).Max();
            var xScaled = new List<double>();
            Console.WriteLine(min);
            Console.WriteLine(max);

            var em = x.GetEnumerator();
            while (em.MoveNext())
            {
                var xStd = (Convert.ToInt32(em.Current) - min * 1.0) / (max - min);
                xScaled.Add(xStd * (to - from) + from);
            }

            return xScaled;
        }

        /**
         * Create Pandas DataFrame from DataTable
         */
        private static pd.DataFrame CreateDataFrameFromDataTable(DataTable tbl)
        {
            var rows = new List<PandasNet.Series>();

            foreach (DataRow row in tbl.Rows)
            {
                rows.Add(new pd.Series(row.ItemArray.Select(p => p.ToString()).ToArray()));
            }

            return new pd.DataFrame(rows);
        }

        /**
         * Read CSV into DataFrame using Deedle library
         */
        private static void ReadCsvIntoDeedleDataFrame(string input_file)
        {
            var df = Frame.ReadCsv(input_file);

            Console.WriteLine(df);

            Console.WriteLine(df.ColumnCount);
            Console.WriteLine(df.Columns);
        }
    }
}
