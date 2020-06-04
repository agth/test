  public static class NLogUtils
    {
		public static void LogEx(this Logger logger, Exception ex)
		{
			LogEx(logger, ex, null);
		}

		public static void LogEx(this Logger logger, Exception ex, string message, params object[] args)
		{
            if (!Logger.IsLoggingEnabled)
                return;

			if (message != null)
			{
				message = string.Format(message, args);
			}

			if (ex == null)
			{
				if (!string.IsNullOrWhiteSpace(message))
				{
				    logger.Error(message);
				}
				return;
			}

			try
			{
				ex = ex.GetBaseException();

				var errorMessage = new StringBuilder();

				if (!string.IsNullOrWhiteSpace(message))
				{
					errorMessage.Append(message);
					errorMessage.Append(' ');
					errorMessage.AppendLine();
				}

				errorMessage.Append('[');

				var sqlex = ex as SqlException;
				if (sqlex != null)
				{
					errorMessage.Append("SQL ERROR: ");
				}
				errorMessage.Append(GetFirstNonSystemSource(ex, false));
				errorMessage.Append("] ");

				if (!AppendSqlMessage(sqlex, errorMessage))
				{
                    errorMessage.Append(ex.Message);
                }

			    logger.Error(ex, errorMessage.ToString());
			}
			catch (Exception ex2)
			{
			    logger.Error(ex.Message);
			    logger.Error(ex2, "LOGGER INTERNAL ERROR");
			}
		}

		public static bool TraceSql(this Logger logger, Exception ex, SqlCommand cmd)
		{
		    if (cmd == null)
		        return false;

		    if (!Logger.IsLoggingEnabled)
		        return true;

		    var errorMessage = new StringBuilder();

		    try
		    {
		        if (ex != null)
		        {
		            if (!AppendSqlMessage(ex as SqlException, errorMessage))
		            {
		                errorMessage.Append($"Error : {ex.Message}");
		            }
		        }

		        if (cmd.Parameters.Count > 0)
		        {
		            if (errorMessage.Length > 0)
		            {
		                errorMessage.Append(" \r\n");
		            }

		            foreach (SqlParameter param in cmd.Parameters)
		            {
		                errorMessage.Append(" \r\n");
		                errorMessage.Append("Declare ");
		                if (!param.ParameterName.StartsWith("@"))
		                {
		                    errorMessage.Append("@");
		                }
		                errorMessage.Append(param.ParameterName);
		                errorMessage.Append(" ");
		                errorMessage.Append(param.SqlDbType);
		                errorMessage.Append(" = ");
		                if (param.Value is string || param.Value is Guid || param.Value is DateTime ||
		                    param.Value is DateTimeOffset)
		                {
		                    var str = param.Value.ToString();
		                    errorMessage.Append('\'');
		                    errorMessage.Append(str.Length > 1024 ? str.Substring(0, 1024) : str);
		                    errorMessage.Append('\'');
		                    if (str.Length > 1024)
		                    {
		                        errorMessage.Append("...");
		                    }
		                }
		                else if (param.Value is bool)
		                {
		                    errorMessage.Append((bool) param.Value ? "1" : "0");
		                }
		                else if (param.Value is Enum)
		                {
		                    errorMessage.Append(Convert.ToInt32(param.Value));
		                    errorMessage.Append(" -- ");
		                    errorMessage.Append(param.Value);
		                }
		                else
		                {
		                    if (param.Value is byte[] bytes)
		                    {
		                        var buff = bytes;
		                        var length = buff.Length > 1024 ? 1024 : buff.Length;

		                        errorMessage.Append("Convert(Varbinary,'");
		                        errorMessage.Append(BitConverter.ToString(buff, 0, length));
		                        errorMessage.Append("')");
		                        if (buff.Length > length)
		                        {
		                            errorMessage.Append("...");
		                        }
		                    }
		                    else
		                    {
		                        errorMessage.Append((param.Value != null && param.Value != DBNull.Value)
		                            ? param.Value.ToString()
		                            : "NULL");
		                    }
		                }
		            }
		        }

		        var s = (cmd.CommandText ?? string.Empty).Trim();

		        if (!string.IsNullOrEmpty(s))
		        {
		            if (errorMessage.Length > 0)
		            {
		                errorMessage.Append(" \r\n\r\n");
		            }

		            errorMessage.Append(s.ClearSpaces(WhiteSpaces));
		        }

		        if (errorMessage.Length > 0)
		        {
		            logger.Trace($" \r\n*** sql trace {DateTime.Now:yyyy-MM-dd HH:mm:ss} *** \r\n{errorMessage} \r\n*** sql trace end *** \r\n");
		        }
		    }
		    catch (Exception ex2)
		    {
		        if (ex != null)
		        {
		            logger.Error(ex.Message);
		        }
		        logger.Error(ex2, "LOGGER INTERNAL ERROR");
		    }

		    return true;
		}

		private static bool AppendSqlMessage(SqlException sqlex, StringBuilder errorMessage)
		{
		    if (sqlex == null)
		        return false;

			if (!string.IsNullOrEmpty(sqlex.Server))
			{
				errorMessage.Append(" \r\nServer '");
				errorMessage.Append(sqlex.Server);
				errorMessage.Append("' ");
			}

			for (var i = 0; i < sqlex.Errors.Count; ++i)
			{
				errorMessage.Append(" \r\nMsg ");
				errorMessage.Append(sqlex.Errors[i].Number);
				errorMessage.Append(", Level ");
				errorMessage.Append(sqlex.Errors[i].Class);
				errorMessage.Append(", State ");
				errorMessage.Append(sqlex.Errors[i].State);
				if (!string.IsNullOrEmpty(sqlex.Errors[i].Procedure))
				{
					errorMessage.Append(", Proc '");
					errorMessage.Append(sqlex.Errors[i].Procedure);
					errorMessage.Append("'");
				}
				errorMessage.Append(", Line ");
				errorMessage.Append(sqlex.Errors[i].LineNumber);
				errorMessage.Append(".  ");
				errorMessage.Append(sqlex.Errors[i].Message);
			}

		    return true;
		}

	    private static string GetFirstNonSystemSource(Exception ex, bool noStackTrace)
		{
			if (string.IsNullOrEmpty(ex.StackTrace))
				return string.IsNullOrEmpty(ex.Source) ? "no_source_or_stacktrace" : ex.Source.Trim();

			var sta = ex.StackTrace.Split(new[] { " at " }, StringSplitOptions.RemoveEmptyEntries);

			var userSource = string.Empty;
			var systemSource = string.Empty;

            foreach (var st in sta)
            {
                var s = st.Trim();

                if (string.IsNullOrEmpty(s) || !char.IsLetter(s[0]) || (s.IndexOf('.') >= s.IndexOf('(')))
                    continue;

                if (string.Compare(s, 0, "System.", 0, 7) == 0 || (string.Compare(s, 0, "Microsoft.", 0, 10) == 0 && s.IndexOf(":line ", StringComparison.OrdinalIgnoreCase) == -1))
                {
                    if (string.IsNullOrEmpty(systemSource))
                    {
                        systemSource = s;
                    }
                }
                else if (string.IsNullOrEmpty(userSource))
                {
                    userSource = s;
                    break;
                }
            }

            return string.IsNullOrEmpty(userSource) ? (noStackTrace ? systemSource.Replace("\r\n", string.Empty) : ex.StackTrace) : userSource.Replace("\r\n", string.Empty);
		}

        private static readonly char[] WhiteSpaces = { '\u0020', '\u0009', '\u000B', '\u000C' };
	}
