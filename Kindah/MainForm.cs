using System;
using System.ComponentModel;
using System.Windows.Forms;
using System.IO;
using System.Diagnostics;
using Kindah.NuFilter;
using UhooIndexer;

namespace Kindah
{
    public partial class MainForm : Form
    {
        Uhoo _uhoo;
        DateTime _indextime;
        /// <summary>
        /// Initializes a new instance of the <see cref="MainForm"/> class.
        /// </summary>
        public MainForm()
        {
            InitializeComponent();
        }

        #region Events

        private void backgroundWorker1_DoWork(object sender, DoWorkEventArgs e)
        {
            var files = e.Argument as string[];
            var wrk = sender as BackgroundWorker;
            var i = 0;
            foreach (var fn in files)
            {
                if (wrk.CancellationPending)
                {
                    e.Cancel = true;
                    break;
                }
                backgroundWorker1.ReportProgress(1, fn);
                try
                {
                    if (_uhoo.IsIndexed(fn) == false)
                    {
                        TextReader tf = new FilterReader(fn);
                        var s = "";
                        if (tf != null)
                            s = tf.ReadToEnd();

                        _uhoo.Index(new myDoc(new FileInfo(fn), s), true);
                    }
                }
                catch { }
                i++;
                if (i > 1000)
                {
                    i = 0;
                    _uhoo.Save();
                }
            }
            _uhoo.Save();
            _uhoo.OptimizeIndex();
        }
        private void backgroundWorker1_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            lblIndexer.Text = "" + e.UserState;
        }
        private void backgroundWorker1_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            btnStart.Enabled = true;
            btnStop.Enabled = false;
            lblIndexer.Text = "" + DateTime.Now.Subtract(_indextime).TotalSeconds + @" sec.";
            MessageBox.Show(@"Indexing done : " + DateTime.Now.Subtract(_indextime).TotalSeconds + @" sec.");
        }
        private void txtSearch_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Enter)
                btnSearch_Click(null, null);
        }
        private void listBox1_DoubleClick(object sender, EventArgs e)
        {
            Process.Start("" + listBox1.SelectedItem);
        }
        private void Form1_FormClosing(object sender, FormClosingEventArgs e)
        {
            if (_uhoo != null)
                _uhoo.Shutdown();
        }

        #region Click
        private void button1_Click_1(object sender, EventArgs e)
        {
            if (txtIndexFolder.Text == "")
            {
                MessageBox.Show(@"Please supply the index storage folder.");
                return;
            }

            _uhoo = new Uhoo(Path.GetFullPath(txtIndexFolder.Text), "index", true);
            button1.Enabled = false;
        }
        private void button2_Click(object sender, EventArgs e)
        {
            if (_uhoo == null)
            {
                MessageBox.Show(@"Uhoo not loaded.");
                return;
            }
            MessageBox.Show(_uhoo.WordCount.ToString("#,#") + @" words in " + _uhoo.DocumentCount.ToString("#,#") + @" documents.", @"Index statistics", MessageBoxButtons.OK);
        }
        private void button4_Click(object sender, EventArgs e)
        {
            if (_uhoo == null)
            {
                MessageBox.Show(@"No Uhoo to save.");
                return;
            }
            _uhoo.Save();
        }
        private void button6_Click(object sender, EventArgs e)
        {
            var fbd = new FolderBrowserDialog();
            fbd.SelectedPath = Directory.GetCurrentDirectory();
            if (fbd.ShowDialog() == DialogResult.OK)
            {
                txtIndexFolder.Text = fbd.SelectedPath;
            }
        }
        private void button7_Click(object sender, EventArgs e)
        {
            var fbd = new FolderBrowserDialog {SelectedPath = Environment.CurrentDirectory};
            if (fbd.ShowDialog() == DialogResult.OK)
            {
                txtWhere.Text = fbd.SelectedPath;
            }
        }
        private void btnSearch_Click(object sender, EventArgs e)
        {
            if (_uhoo == null)
            {
                MessageBox.Show(@"Uhoo not loaded.");
                return;
            }

            listBox1.Items.Clear();
            var dt = DateTime.Now;
            listBox1.BeginUpdate();

            foreach (var d in _uhoo.FindDocumentFileNames(txtSearch.Text))
            {
                listBox1.Items.Add(d);
            }
            listBox1.EndUpdate();
            lblStatus.Text = @"Search results: " + listBox1.Items.Count + @" items, in a search time of " + DateTime.Now.Subtract(dt).TotalSeconds + @" s.";
        }
        private void btnStart_Click(object sender, EventArgs e)
        {
            if (txtIndexFolder.Text == "" || txtWhere.Text == "")
            {
                MessageBox.Show(@"Please supply the index storage folder and the where to start indexing from.");
                return;
            }

            btnStart.Enabled = false;
            btnStop.Enabled = true;
            if (_uhoo == null)
                _uhoo = new Uhoo(Path.GetFullPath(txtIndexFolder.Text), "index", true);

            var files = Directory.GetFiles(txtWhere.Text, "*", SearchOption.AllDirectories);
            _indextime = DateTime.Now;
            backgroundWorker1.RunWorkerAsync(files);
        }
        private void btnStop_Click(object sender, EventArgs e)
        {
            backgroundWorker1.CancelAsync();
        }
        #endregion

        #endregion
    }
}
