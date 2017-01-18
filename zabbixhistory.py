import argparse
import csv
import getpass
import sys
import time,datetime
import math

from pyzabbix import ZabbixAPI


def get_zapi(host, user, password, verify):
	"""

	:param host:
	:param user:
	:param password:
	:param verify:
	:return:
	"""
	zapi = ZabbixAPI(host)
	# Whether or not to verify the SSL certificate
	zapi.session.verify = verify
	zapi.login(user, password)
	return zapi


def convert_size(size):
	if size == 0:
		return '0b'
	size_name = ('bps', 'Kbps', 'Mbps', 'Gbps')
	i = int(math.floor(math.log(size,1000)))
	p = math.pow(1000,i)
	s = round(size/p,2)
	if str(s).endswith('0'):
		return '%g'%(s)+" "+size_name[i]
	return '%s %s' % (s,size_name[i])

def get_history(zapi, itemid, time_from, time_till):
	"""
	The zabbix api call for history.get requires that we know the item's
	data type. We can get this through a call to the zabbix api since we
	have the itemid.
	:param zapi:
	:param itemid:
	:param time_from:
	:param time_till:
	:return:
	"""
	aliasid = itemid['aliasid']
	del itemid['aliasid']
	speedid = itemid['speedid']
	del itemid['speedid']
	valuemax = itemid['valuemax']
	del itemid['valuemax']
	valuemax_out = itemid['valuemax_out']
	del itemid['valuemax_out']
	trafficoutid = itemid['trafficout']
	del itemid['trafficout']
	items = zapi.item.get(itemids=itemid, output=['value_type'])
	ret = []
	alias = []
	speed = []
	trafficout = []
	# The only successful outcome would be a list with one item. If we get
	# more or less, we should assume that the item doesn't exist.
	if len(items) == 1:
		value_type = items[0]['value_type']
	else:
		raise Exception('Item not found')

	ret += zapi.history.get(itemids=itemid,time_from=time_from,time_till=time_till,history=value_type,sortfield='clock',output='extend')
	trafficout += zapi.history.get(itemids=trafficoutid,time_from=time_from,time_till=time_till,history=value_type,sortfield='clock',output='extend')
	if len(trafficout) != 0:
		ret[0]['trafficout'] = trafficout[0]['value']
	alias += zapi.history.get(itemids=aliasid,time_from=time_from,time_till=time_till,history=4,sortfield='clock',output='extend')
	if len(alias) != 0:
		ret[0]['alias'] = alias[0]['value']
	speed += zapi.history.get(itemids=speedid,time_from=time_from,time_till=time_till,history=value_type,sortfield='clock',output='extend')
	if len(speed) != 0:
		ret[0]['speed'] = speed[0]['value']
	if len(ret) != 0:
		ret[0]['valuemax'] = unicode(valuemax)
		ret[0]['valuemax_out'] = unicode(valuemax_out)
	return ret


def write_csv(objects, output_file):
	"""
	:param objects:
	:param output_file:
	:param hostid:
	:return:
	"""
	print 'Writing CSV...'
	# Open the output_file and instanstiate the csv.writer object
	f = csv.writer(open(output_file, 'wb+'),dialect=csv.excel,delimiter=';')

	# Write the top line of the output_file which descibes the columns
	f.writerow(['Host','Port','Speed','Alias','Traffic in','Traffic out','Max speed in','Max speed out'])
	# For each object, write a row to the csv file.
	for o in objects:
		row = []
		row.append(zapi.host.get(output=["name"],itemids=o['itemid'])[0]['name'])
		temp = []
		temp += zapi.item.get(output=["name"],itemids=o['itemid'])
		for n in temp:
			row.append(' '+n['name'][-4:].replace(" ", ""))
		if o['value'].isdigit() == True:
			row.append(convert_size(int(o['speed'])))
			row.append(o['alias'])
			row.append(convert_size(int(o['value'])))
			row.append(convert_size(int(o['trafficout'])))
		else:
			row.append(o['value'])
		row.append(convert_size(int(o['valuemax'])))
		row.append(convert_size(int(o['valuemax_out'])))
		f.writerow(row)


def build_parsers():
	"""
	Builds the argparser object
	:return: Configured argparse.ArgumentParser object
	"""
	parser = argparse.ArgumentParser(
			formatter_class=argparse.ArgumentDefaultsHelpFormatter,
			description='zabbixhistory'
	)
	parser.add_argument('-V', '--verify',
						default='True',
						choices=['True', 'False'],
						help='Verify SSL (True, False)')
	parser.add_argument('-H', '--host',
						dest='host',
						required=True,
						help='Zabbix API host'
							 'example: http://zabbixhost.example.com/zabbix')
	parser.add_argument('-u', '--user',
						default=getpass.getuser(),
						help='Zabbix API user')
	parser.add_argument('-d', '--date',
						default="03/12/2016",
						type=str,
						help='Date in format dd/mm/yy')
	parser.add_argument('-c', '--clock',
						default=20,
						type=int,
						help='Start time in hours, format: 20')
	parser.add_argument("-o", "--output-file",
						default='output.csv',
						help='Output file in csv format\nDefault: output.csv')
	parser.add_argument('-i', '--groupid',
						required=True,
						help='The zabbix group id that we will use '
							 'in our history.get api call.')
	parser.add_argument('-t', '--time_minutes',
						default=5,
						type=int,
						help='Amount of minutes, default: 5')

	return parser


if __name__ == '__main__':
	# Load argparse and parse arguments

	hosts_arr = []
	items_tr_in = []
	items_tr_out = []
	items_alias = []
	items_speed = []
	all_results = []

	parser = build_parsers()
	args = parser.parse_args(sys.argv[1:])

	# Generate parameters for get_zapi function
	password = getpass.getpass()
	time_start = int(datetime.datetime.strptime(args.date, '%d/%m/%Y').strftime('%s'))+(args.clock * 3600)
	time_end = time_start + (args.time_minutes * 60)
	time_now = int(time.time())
	# Generate the zapi object so we can pass it to the get_history function
	try:
		zapi = get_zapi(args.host, args.user, password, eval(args.verify))
	except Exception as e:
		if 'Login name or password is incorrect.' in str(e):
			print('Unauthorized: Please check your username and password')
		else:
			print('Error connecting to zabbixapi: {0}'.format(e))
		exit()

	# generate the list of history objects returned from zabbix api.
	try:
		hosts_arr += zapi.host.get(output='["hostid"]',groupids=args.groupid)
		print 'Collecting info:'
		for it_host in hosts_arr:
			items_tr_in += zapi.item.get(output=["name"],hostids=it_host,search={"name": "Traffic in"},sortfield='itemid')
			items_alias += zapi.item.get(output=["itemid"],hostids=it_host,search={"name": "Alias of interface"},sortfield='itemid')
			items_speed += zapi.item.get(output=["itemid"],hostids=it_host,search={"name": "Speed of interface"},sortfield='itemid')
			items_tr_out += zapi.item.get(output=["name"],hostids=it_host,search={"name": "Traffic out"},sortfield='itemid')
			
			print 'Current host ID: '+it_host['hostid']
			i = 0
			for it_item in items_tr_in:
				valuemax = []
				valuemax_out = []
				it_item['aliasid'] = items_alias[i]['itemid']
				it_item['speedid'] = items_speed[i]['itemid']
				it_item['trafficout'] = items_tr_out[i]['itemid']
				valuemax += zapi.trend.get(itemids=it_item['itemid'],time_from=time_now-604800,time_till=time_now,output=["value_max"])
				valuemax_out += zapi.trend.get(itemids=items_tr_out[i]['itemid'],time_from=time_now-604800,time_till=time_now,output=["value_max"])
				if len(valuemax) != 0:
					temp = []
					for value in valuemax:
						temp.append(int(value['value_max']))
					temp2 = []
					for value in valuemax_out:
						temp2.append(int(value['value_max']))
					it_item['valuemax'] = max(temp)
					it_item['valuemax_out'] = max(temp2)
				else:
					it_item['valuemax'] = unicode('')
					it_item['valuemax_out'] = unicode('')
				results = get_history(zapi, it_item, time_start, time_end)
				all_results += results
				i += 1
			items_tr_in = []
			items_tr_out = []
			items_alias = []
			items_speed = []
	except Exception as e:
		message = ('An error has occurred.'
				   '\nError:\n{0}')
		print(message.format(e))
		exit()

	# Write the results to file in csv format
	write_csv(all_results, args.output_file)
	print('Writing {0} minutes worth of history to {1}'.format(
			args.time_minutes, args.output_file))
