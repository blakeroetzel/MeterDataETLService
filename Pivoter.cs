using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace MeterDataETLService
{
    class Pivoter
    {
        private long MeterID { get; set; }
        private DateTime FirstInterval { get; set; }
        public List<DataRow> dataRows = new List<DataRow>();

        public Pivoter() { }

        public Pivoter(long meterId, string firstInterval, DataTable dt)
        {
            MeterID = meterId;
            FirstInterval = DateTime.ParseExact(firstInterval, "MMddyyyyhhmmsstt", null);
            for (int i = 0; i < 96; i++)
            {
                DataRow newRow = dt.NewRow();
                newRow["MeterID"] = MeterID;
                newRow["Interval_Start"] = FirstInterval.AddMinutes((i - 1) * 15);
                newRow["Interval_End"] = FirstInterval.AddMinutes(i * 15);
                dataRows.Add(newRow);
            }
        }

        public void addData(string d, int index, string delRec)
        {
            int myIndex = ((index - 15) / 2);
            Double z;
            if (d == "")
            {
                z = 0;
            }
            else
            {
                z = Double.Parse(d);
            }
            dataRows[myIndex][delRec] = z;
        }
    }

}
