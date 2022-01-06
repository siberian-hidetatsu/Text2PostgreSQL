using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.IO;
using log4net;
using System.Reflection;
using Npgsql;

namespace Text2PostgreSQL
{
	class Program
	{
		private static readonly ILog logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

		// カラム情報
		struct colinfo
		{
			public string name;	// カラム名
			public string type;	// データ型
			public string key;	// yes: ユニーク キー
		}

		static void Main(string[] args)
		{
			try
			{
				#region log4net の動的設定
				try
				{
					// C#, log4net なんだこの使いにくさは
					// http://blog.livedoor.jp/nanoris/archives/51868711.html

					string logPath = ".";
#if (DEBUG)
					logPath = @"C:\TEMP\Text2PostgreSQL";
#endif

					// Loggerの生成
					var logger = LogManager.GetLogger("mylogger");

					// RootのLoggerを取得
					var rootLogger = ((log4net.Repository.Hierarchy.Hierarchy)logger.Logger.Repository).Root;

					// RootのAppenderを取得
					var appender = rootLogger.GetAppender("RollingFileAppender") as log4net.Appender.FileAppender;

					// ファイル名の取得
					var filepath = appender.File;

					// ファイル名の設定
					//appender.File = dataDirectoryPath + @"\log\kellogg";		// 日付形式の場合
					appender.File = logPath + @"\log\trace.log";    // サイズ形式の場合
					appender.ActivateOptions();
				}
				catch ( Exception exp )
				{
					Console.WriteLine(exp.Message);
				}
				#endregion

				// テキストファイル情報を読み込む
				XmlDocument textFileInfoXml = new XmlDocument();
				textFileInfoXml.Load(@".\textFileInfo.xml");

				foreach ( XmlElement textFileInfo in textFileInfoXml.DocumentElement.ChildNodes )
				{
					#region 処理するファイルの情報
					string path = textFileInfo["path"].InnerText;
					string searchPattern = textFileInfo["searchPattern"].InnerText;
					string delimiter = textFileInfo["delimiter"].InnerText;

					logger.Info(path + @"\" + searchPattern);

					string backupPath = path + @"\backup files";
					if ( !Directory.Exists(backupPath) )
					{
						Directory.CreateDirectory(backupPath);
					}

					string tableName = textFileInfo["table"].Attributes["name"].Value;
					string columns = textFileInfo["table"].Attributes["columns"].Value;
					colinfo[] colinfo = new colinfo[0];
					bool hasUniqueKey = false;

					foreach ( var column in columns.Split(',') )
					{
						// カラム名:データ型:ユニーク キー
						string[] values = column.Split(':');

						Array.Resize(ref colinfo, colinfo.Length + 1);
						colinfo[colinfo.Length - 1].name = values[0];
						colinfo[colinfo.Length - 1].type = values[1];
						colinfo[colinfo.Length - 1].key = values[2];

						if ( values[2] == "yes" )
						{
							hasUniqueKey = true;
						}
					}
					#endregion

					#region テーブル情報
					XmlDocument tableInfoXml = new XmlDocument();
					tableInfoXml.Load(@".\tableInfo.xml");

					string xpath = $"/root/table[@name='{tableName}']";
					XmlElement tableInfo = (XmlElement)tableInfoXml.SelectSingleNode(xpath);

					string server = tableInfo["server"].InnerText;
					string port = "5432";
					if ( tableInfo.Attributes["port"] != null )
					{
						port = tableInfo.Attributes["port"].Value;
					}
					string userId = tableInfo["userId"].InnerText;
					string password = tableInfo["password"].InnerText;
					string database = tableInfo["database"].InnerText;
					string conStr = $"Server={server};Port={port};User Id={userId};Password={password};Database={database};";

					if ( tableInfo["timeout"] != null )
					{
						conStr += $"Timeout={tableInfo["timeout"].InnerText};";
					}

					if ( tableInfo["commandTimeout"] != null )
					{
						conStr += $"CommandTimeout={tableInfo["commandTimeout"].InnerText};";
					}
					#endregion

					using ( NpgsqlConnection npgsqlConn = new NpgsqlConnection(conStr) )
					{
						string result = "";

						npgsqlConn.Open();

						// ファイルの一覧を取得する
						var fileNames = new DirectoryInfo(path)
							.GetFiles(searchPattern)
							.OrderBy(fi => fi.CreationTime)
							.Select(fi => fi.FullName);

						foreach ( string fileName in fileNames )
						{
							try
							{
								//Console.WriteLine($"{fileName}");
								char _delimiter = (delimiter == "tab") ? '\t' : delimiter[0];

								StringBuilder cmdText = null;

								foreach ( string line in File.ReadLines(fileName, Encoding.GetEncoding("shift_jis")) )
								{
									try
									{
										string[] values = line.Split(_delimiter);

										if ( colinfo[colinfo.Length - 1].name == "update" )
										{
											Array.Resize(ref values, values.Length + 1);
											values[values.Length - 1] = DateTime.Now.ToString("yyyyMMdd HHmmss");
										}

										if ( values.Length != colinfo/*column_names*/.Length )
										{
											Console.WriteLine("データのカラム数不一致");
											logger.Warn($"データのカラム数不一致 {line}");
											continue;
										}

										cmdText = new StringBuilder();

										// 挿入
										cmdText.Append($"insert into {tableName} (");
										for ( int i = 0; i < colinfo.Length; i++ )
										{
											cmdText.Append(i == 0 ? "" : ",");
											cmdText.Append(colinfo[i].name);
										}
										cmdText.Append(") ");

										cmdText.Append("values (");
										for ( int i = 0; i < values.Length; i++ )
										{
											cmdText.Append(i == 0 ? "" : ",");
											string quotation = (colinfo[i].type.StartsWith("char") ? "'" : "");
											cmdText.Append($"{quotation}{values[i]}{quotation}");
										}
										cmdText.Append(")");

										if ( hasUniqueKey )
										{
											// PostgreSQLで「あればUPDATE、なければINSERT」のUPSERTをやってみる
											// https://blog.officekoma.co.jp/2018/06/postgresqlupdateinsertupsert.html

											// ユニーク キー
											string unique_key = "";
											for ( int i = 0; i < colinfo.Length; i++ )
											{
												if ( colinfo[i].key == "yes" )
												{
													unique_key += (unique_key.Length == 0 ? "" : ",");
													unique_key += colinfo[i].name;
												}
											}
											cmdText.Append($"\r\non conflict({unique_key})");

											// 更新
											cmdText.Append($"\r\ndo update set ");
											StringBuilder update_set = new StringBuilder();
											for ( int i = 0; i < colinfo.Length; i++ )
											{
												if ( colinfo[i].key != "yes" )
												{
													update_set.Append(update_set.Length == 0 ? "" : ",");
													string quotation = (colinfo[i].type.StartsWith("char") ? "'" : "");
													update_set.Append($"{colinfo[i].name}={quotation}{values[i]}{quotation}");
												}
											}
											cmdText.Append(update_set);
										}

										using ( NpgsqlCommand npgsqlCommand = new NpgsqlCommand(cmdText.ToString(), npgsqlConn) )
										{
											var count = npgsqlCommand.ExecuteNonQuery();
										}
									}
									catch ( Exception exp )
									{
										Console.WriteLine(exp.Message);
										logger.Error(exp.Message);
										logger.Error(cmdText.ToString());
									}
								}
							}
							catch ( Exception exp )
							{
								Console.WriteLine(exp.Message);
								logger.Error(exp.Message);
								result = "_error";
							}
							finally
							{
								string destFileName = backupPath + @"\" + Path.GetFileNameWithoutExtension(fileName) + "_" + DateTime.Now.ToString("yyyyMMddHHmmss") + result + Path.GetExtension(fileName);
								if ( File.Exists(destFileName) )
								{
									File.Delete(destFileName);
								}
								File.Move(fileName, destFileName);
							}
						}

						npgsqlConn.Close();
					}
				}
			}
			catch ( Exception exp )
			{
				Console.WriteLine(exp.Message);
				logger.Error(exp.Message);
			}
		}
	}
}
