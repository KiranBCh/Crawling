import os, scrapy, re, json, csv, requests
import datetime, time, os
from scrapy.http import FormRequest, Request
from scrapy.selector import Selector
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
#from collections import OrderedDict
from dateutil import parser, relativedelta
from utils import *

PAR_DIR = os.path.abspath(os.pardir)
OUTPUT_DIR = os.path.join(PAR_DIR, 'spiders/OUTPUT')
PROCESSING_QUERY_FILES_PATH = os.path.join(OUTPUT_DIR, 'Processing')

#Username - vasu.gaurav@gmail.com
#Password - Unearth@2018

#UserName = hyperxtreme007@gmail.com
#password = akshay2018

class LinkedinpremiumGaurav(scrapy.Spider):
	name = "linkedindetails_sp"
	allowed_domains = ["linkedin.com"]
	handle_httpstatus_list = [404]
	start_urls = ['https://www.linkedin.com/uas/login?goback=&trk=hb_signin']
	
	def __init__(self, filenames, *args, **kwargs):
		super(LinkedinpremiumGaurav, self).__init__(*args, **kwargs)
		self.login = kwargs.get('login', 'Gaurav')
		self.keyword = kwargs.get('key', 'hackerrank')
		self.logins_dict = {'Gaurav': ['hyperxtreme007@gmail.com', 'akshay2018']}
		dispatcher.connect(self.spider_closed, signals.spider_closed)
		self.domain = "https://www.linkedin.com"
		file_name = self.name.split('_')[0] +'_%s' % str(datetime.datetime.now().strftime("%Y%m%dT%H%M%S%f"))
		file_path = PROCESSING_QUERY_FILES_PATH + '/%s.txt' % file_name
		self.output_file = open(file_path, 'w+')
		self.filenames_path = filenames

	def parse(self, response):
		sel = Selector(response)
		logincsrf = ''.join(sel.xpath('//input[@name="loginCsrfParam"]/@value').extract())
		csrf_token = ''.join(sel.xpath('//input[@name="csrfToken"]/@value').extract())
		loginCsrfParam = ''.join(sel.xpath('//input[@name="loginCsrfParam"]/@value').extract())
		sIdString = ''.join(sel.xpath('//input[@name="sIdString"]/@value').extract())
		pageInstance = ''.join(sel.xpath('//input[@name="pageInstance"]/@value').extract())
		login_account = self.logins_dict[self.login]
		account_mail, account_password = login_account
		data = {
		  'csrfToken': csrf_token,
		  'session_key': account_mail,
		  'ac': '0',
		  'sIdString': sIdString,
		  'controlId': 'd_checkpoint_lg_consumerLogin-login_submit_button',
		  'parentPageKey': 'd_checkpoint_lg_consumerLogin',
		  'pageInstance': 'urn:li:page:d_checkpoint_lg_consumerLogin;tIx+gvgpR0yczg9tQXc1Sw==',
		  'trk': 'hb_signin',
		  'session_redirect': '',
		  'loginCsrfParam': loginCsrfParam,
		  '_d': 'd',
		  'session_password': account_password,
		}
		headers = {
				'cookie': response.headers.getlist('Set-Cookie'),
				'origin': 'https://www.linkedin.com',
				'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
				'x-requested-with': 'XMLHttpRequest',
				'x-isajaxform': '1',
				'accept-encoding': 'gzip, deflate, br',
				'pragma': 'no-cache',
				'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36',
				'content-type': 'application/x-www-form-urlencoded',
				'accept': '*/*',
				'cache-control': 'no-cache',
				'authority': 'www.linkedin.com',
				'referer': 'https://www.linkedin.com/',
			}
		url = 'https://www.linkedin.com/checkpoint/lg/login-submit'
		yield FormRequest(url, callback=self.parse_next, formdata=data, headers = headers, meta = {"csrf_token":csrf_token})

	def is_path_file_name(self, excel_file_name):
		if os.path.isfile(excel_file_name):
			os.system('rm %s' % excel_file_name)
		oupf = open(excel_file_name, 'ab+')
		todays_excel_file = csv.writer(oupf)
		return todays_excel_file
	
	def spider_closed(self, spider):
		cv = requests.get('https://www.linkedin.com/logout/').text


	def parse_next(self, response):
		sel = Selector(response)
		cooki_list = response.request.headers.get('Cookie', []).decode('utf-8')
		li_at_cookie = ''.join(re.findall('li_at=(.*?); ', cooki_list))
		headers = {
                    'cookie': 'li_at=%s;JSESSIONID="%s"' % (li_at_cookie, response.meta['csrf_token']),
                    'x-requested-with': 'XMLHttpRequest',
                    'csrf-token': response.meta['csrf_token'],
                    'authority': 'www.linkedin.com',
                    'referer': 'https://www.linkedin.com/',
             }
		with open(self.filenames_path, 'r') as f:
			lines = f.readlines()
			for url in lines:
				if url:
					url = normalize(url.replace("\"", ''))
					url_id = url.split('/')[-1]
					html_url = "https://www.linkedin.com/company/%s/"% url_id
					yield Request(html_url, callback = self.parse_correcthtml, 
						meta={'csrf_token':response.meta['csrf_token'], 'headers':headers, "companyName":html_url},
						headers=headers)
					
	def parse_correcthtml(self, response):
		comp_id = ''
		try:
			comp_id = list(set(re.findall('\[\&\quot\;urn\:li\:fs_normalized_company\:(\d+)\&quot\;\]', response.body)))[0]
		except:
			comp_id = ''
		if comp_id:
			headers = response.meta.get('headers', '')
			api_compid_url = "https://www.linkedin.com/voyager/api/organization/companies?decoration=(adsRule,affiliatedCompaniesWithEmployeesRollup,affiliatedCompaniesWithJobsRollup,articlePermalinkForTopCompanies,autoGenerated,backgroundCoverImage,claimable,claimableByViewer,companyEmployeesSearchPageUrl,companyPageUrl,confirmedLocations*,coverPhoto,dataVersion,description,entityUrn,followingInfo,foundedOn,headquarter,jobSearchPageUrl,lcpTreatment,logo,name,type,overviewPhoto,paidCompany,partnerCompanyUrl,partnerLogo,partnerLogoImage,permissions,rankForTopCompanies,salesNavigatorCompanyUrl,school,showcase,staffCount,staffCountRange,staffingCompany,topCompaniesListName,universalName,url,viewerConnectedToAdministrator,viewerEmployee,viewerFollowingJobsUpdates,viewerPendingAdministrator,companyIndustries*,industries,specialities,acquirerCompany~(entityUrn,logo,name,industries,followingInfo,url,paidCompany,universalName),affiliatedCompanies*~(entityUrn,logo,name,industries,followingInfo,url,paidCompany,universalName),groups*~(entityUrn,largeLogo,groupName,memberCount,websiteUrl,url),showcasePages*~(entityUrn,logo,name,industries,followingInfo,url,description,universalName))&q=universalName&universalName=%s" % comp_id
			yield Request(api_compid_url, callback = self.parse_correct, \
					meta = {'csrf_token': response.meta['csrf_token'], 'headers':headers, "comp_id":comp_id,\
					"companyName":response.meta.get("companyName")}, headers = headers)

	def parse_correct(self, response):
		data = json.loads(response.body)
		comp_id = response.meta.get('comp_id', '')
		basic_company_details = data.get('elements', {})
		headers = response.meta.get('headers', {})
		CompanyInformation = {}
		if basic_company_details:
			basic_company_details = basic_company_details[0]
			company_name = basic_company_details.get("name", '')
			about_us = basic_company_details.get('description', '')
			specialities = basic_company_details.get('specialities', '')
			company_url = basic_company_details.get("companyPageUrl")
			num_of_Locations = basic_company_details.get("confirmedLocations")
			yearFounded = basic_company_details.get("foundedOn")
			companySize = basic_company_details.get("staffCountRange")
			headquarters = basic_company_details.get("headquarter")
			total_employess_linkedin = str(basic_company_details.get('staffCount', ''))
			companyFollowers_count = basic_company_details.get('followingInfo', {}).get('followerCount', '')
			CompanyInformation.update({
				"companyName":company_name, "aboutUs":about_us, "specialities":specialities,
				"companyUrl":company_url, "num_of_Locations":num_of_Locations,
				"companyNameQuery":response.meta.get("companyName"),
				"yearFounded":yearFounded, "companySize":companySize, "headquarters":headquarters,
				"total_employess_linkedin":total_employess_linkedin, "CompanyId":comp_id,
				"companyFollowers_count":companyFollowers_count, "PersonNames":[],
				"RunningDate":str(datetime.datetime.now().date()),
				"json_response_about":basic_company_details,
			})
		url = "https://www.linkedin.com/voyager/api/organization/premiumInsights/urn%3Ali%3Afs_normalized_company%3A"+comp_id+"?decorationId=com.linkedin.voyager.deco.organization.web.premium.WebFullPremiumInsights-4"
		yield Request(url,  callback = self.parse_insights, \
			meta = {'csrf_token': response.meta['csrf_token'], 'headers':headers,\
					"comp_id":comp_id, "CompanyInformation":CompanyInformation}, headers = headers)

	def parse_insights(self, response):
		CompanyInformation = response.meta.get('CompanyInformation', '')
		comp_id = response.meta.get('comp_id', '')
		headers = response.meta.get('headers', {})
		metainformation = json.loads(response.body)
		totalEmployeeCount = {}
		#------------------------Total employee count-----------------
		averageTenure = metainformation.get("headcountInsights", {}).get("averageTenureYears")
		CompanyInformation["averageTenure"] = averageTenure
		currentTotalEmployeesCount = metainformation.get("headcountInsights", {}).get("totalEmployees")
		CompanyInformation["currentTotalEmployeesCount"] = currentTotalEmployeesCount
		growthPeriods = metainformation.get("headcountInsights", {}).get("growthPeriods", [])
		Six_To_Two_Years_Growth = []
		for growth in growthPeriods:
			Six_To_Two_Years_Growth.append(growth)
		totalEmployeeCount.update({"growthPeriods":Six_To_Two_Years_Growth})
		CompanyInformation["Six_To_Two_Years_Growth"] = Six_To_Two_Years_Growth
		YearWise_Growth = []
		growthPeriodsHeadCounts	 = metainformation.get("headcountInsights", {}).get("headcounts", [])
		for headcounts in growthPeriodsHeadCounts:
			employeeCount = headcounts.get("employeeCount")
			year1 = headcounts.get("yearMonthOn").get("year")
			month1 = headcounts.get("yearMonthOn").get("month")
			day1 = headcounts.get("yearMonthOn").get("day")
			posted_date = "%s-%s-%s" % (year1,month1,day1)
			if posted_date:
				posted_date = str(parser.parse(posted_date).date())
				YearWise_Growth.append({"employeeCount":employeeCount, "date":posted_date})
		CompanyInformation["YearWise_Growth"] = YearWise_Growth
		#-------------------------New hires---------------------------------------
		SeniorManagementFinal = []
		try:
			SeniorManagementList = metainformation.get("hiresInsights").get("hireCounts", [])
		except:
			SeniorManagementList = []
			SeniorManagementFinal = []
			pass
		for SeniorManagement in SeniorManagementList:
			SeniorManagement1  = SeniorManagement.get("seniorHireCount")
			allEmployeeHireCount = SeniorManagement.get("allEmployeeHireCount")
			year2 = SeniorManagement.get("yearMonthOn").get("year")
			month2 = SeniorManagement.get("yearMonthOn").get("month")
			day2 = SeniorManagement.get("yearMonthOn").get("day")
			posted_date = "%s-%s-%s" % (year2, month2, day2)
			if posted_date:
				posted_date = str(parser.parse(posted_date).date())
				SeniorManagementFinal.append({"SeniorManagement":SeniorManagement1, "date":posted_date,"allEmployeeHireCount":allEmployeeHireCount})
		CompanyInformation["SeniorManagementHires"] = SeniorManagementFinal
		#-------------------Employee distribution and headcount growth by function--------------
		TotalCountListDate = {}	
		try:
			TotalCountListDate = metainformation.get("functionHeadcountInsights").get("latestHeadcountByFunction").get("yearMonthOn")
			TotalCountList = metainformation.get("functionHeadcountInsights").get("latestHeadcountByFunction").get("totalCount") 
		except: 
			TotalCountListDate = {}
			TotalCountList = ''
			pass
		EmployeeDistributionList = []
		total_records = {}
		if metainformation.has_key("functionHeadcountInsights"):
			nodes = metainformation.get("functionHeadcountInsights")
			HeadcountGrowthList = nodes.get("headcountGrowthByFunction", [])
			FunctionalDistributionList = nodes.get("latestHeadcountByFunction", {}).get("countByFunction", [])
			for HeadcountGrowth, FunctionalDistribution in zip(sorted(HeadcountGrowthList), sorted(FunctionalDistributionList)):
				functionId = HeadcountGrowth.get("function")
				funcationId1 = FunctionalDistribution.get("function")
				if functionId == funcationId1:
					FuncationName = normalize(FunctionalDistribution.get("functionResolutionResult", {}).get("localizedName").replace(" ", "_").strip())
					SixMonthsGrowth = HeadcountGrowth.get("growthPeriods")[0].get("changePercentage")
					OneYearGrowth = HeadcountGrowth.get("growthPeriods")[1].get("changePercentage")
					FuncationPercentage = FunctionalDistribution.get("functionPercentage")
					FunctionCount = FunctionalDistribution.get("functionCount")
					total_records.update({FuncationName:[{"SixMonthsGrowth":SixMonthsGrowth, 
						    "OneYearGrowth":OneYearGrowth,
						    "FuncationPercentage":FuncationPercentage,
						    "FunctionCount":FunctionCount}]})
		CompanyInformation["TotalEmployeeDistribution"] = [{"TotalCountList":TotalCountList,"yearMonthOn":TotalCountListDate}]
		CompanyInformation["EmployeeDistribution_HeadcountByFunction"] = total_records
		#-----------------------Total job openings-----------------------

		TotalJobOpeningsList = []
		try:TotalJobOpenings = metainformation.get("jobOpeningsInsights", {}).get("jobOpeningsByFunctions", [])[0].get("countByFunction", [])
		except Exception as e:
			TotalJobOpenings = []
			pass
		TotalJobOpeningsFinal = {}	
		for TOpenings in TotalJobOpenings:
			FunctionT = TOpenings.get("function")
			functionCount = TOpenings.get("functionCount")
			functionPercentage = TOpenings.get("functionPercentage")
			localizedName = TOpenings.get("functionResolutionResult", {}).get("localizedName")
			TotalJobOpeningsList.append({FunctionT:{"functionCount":functionCount,
				"functionPercentage":functionPercentage, "localizedName":localizedName}})
		JobOpeningsGrowthList1 = []
		JobOpeningsGrowthList = metainformation.get("jobOpeningsInsights", {}).get("jobOpeningsGrowthByFunction", [])
		for JobOpeningsGrowth in JobOpeningsGrowthList:
			functionO = JobOpeningsGrowth.get("function")
			growthPeriodsO = JobOpeningsGrowth.get("growthPeriods")
			JobOpeningsGrowthList1.append({functionO:{"growthPeriods":growthPeriodsO}})
		FinalListEmployeeDistribution1 = []
		for list1 in TotalJobOpeningsList:
			matchKey = list1.keys()
			details_list2 = list1.get("".join(matchKey))
			for list2 in JobOpeningsGrowthList1:
				matchKey2 = list2.keys()
				details_list3 = list2.get("".join(matchKey2))
				if matchKey2 == matchKey:
					key_name = details_list2.get("localizedName")
					FinalName = key_name.replace(" ", "_")
					FinalListEmployeeDistribution1.append({FinalName:{"FunctionalDistribution":details_list2, "JobOpeningsGrowth":details_list3}})	
		try:
			JobCount = (metainformation.get("jobOpeningsInsights", {}).get("jobOpeningsByFunctions", [])[0]).get("totalCount")
			JobDate = metainformation.get("jobOpeningsInsights", {}).get("jobOpeningsByFunctions", [])[0].get("yearMonthOn")
		except:
			JobCount, JobDate = '', ''
			pass
		FinalListEmployeeDistribution1.append({"TotalCountList":JobCount, "yearMonthOn":JobDate})
		CompanyInformation["TotalJobOpenings"] = FinalListEmployeeDistribution1
		self.output_file.write("%s \n" % json.dumps(CompanyInformation))
		import pdb;pdb.set_trace()
