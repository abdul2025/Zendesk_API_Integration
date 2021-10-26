
import datetime
import json
from urllib.parse import urlencode
import requests
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import Column, String, Integer, Date, BigInteger, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import false, null, true
from sqlalchemy.sql.schema import UniqueConstraint  
import datetime
import json


### updating or solving tickets
def updateAndCloseTickets(ticket_id, status, publicStatus, comment):

    credentials = '', ''
    session = requests.Session()
    session.auth = credentials

    ### Public  = True or False for internal or public communication with the PAX
    public = publicStatus
    


    ### updating tickets, status=open, close ticket status=solved 
    #### system will close the ticket after a period of time.
    'open, pending, solved'
    status = status
    ticket_id = ticket_id

    updateData = {
        "ticket": {
            "comment": {
            "body": f"{comment}",
            "public": public
            },
            "status": f"{status}"
        }
    }

    headers = {
        "Content-Type": "application/json"
        }

    print(json.dumps(updateData))
    url = f'{ticket_id}.json'


    
    response = session.put(url, data=json.dumps(updateData), headers=headers, verify=False)
    data = response.json()
    print(response.status_code)

    if response.status_code == 200:
        print('Ticket Updated or solved')
        ######## Update intenal DB (Ticket return to the Zendesk for manual process / further process )
    else:
        print('Ticket not updated or solved')









def connectionDB():
    
    db_string = ''

    engine = create_engine(db_string) 

    
    base = declarative_base()

    class Zendesk_flight_cancelled_ticket(base):
        __tablename__ = 'zendesk_flight_cancelled_ticket'

        id = Column(Integer, primary_key=True, autoincrement=True)
        
        ticket_id = Column(BigInteger, nullable=True)
        PNR = Column(String, nullable=True)
        created_at = Column(String, nullable=True)
        description = Column(String, nullable=True)
        priority = Column(String, nullable=True)
        requester_id = Column(BigInteger, nullable=True)
        status = Column(String, nullable=True)
        subject = Column(String, nullable=True)
        tags = Column(String, nullable=True)
        updated_at = Column(String, nullable=True)
        url = Column(String, nullable=True)
        flightDate = Column(String, nullable=True)
        RefundReason = Column(String, nullable=True)
        flightNum = Column(String, nullable=True)
        paymentMethod = Column(String, nullable=True)
        record_status = Column(String, nullable=True)
        approval_header_id = Column(Integer, nullable=True)
        reoundTripStatus = Column(Integer, nullable=True)
        
        __table_args__ = (UniqueConstraint('ticket_id', name='ticket_id'), )


        
    if not engine.dialect.has_table(engine, 'zendesk_flight_cancelled_ticket', schema=None):
        base.metadata.create_all(engine)

    Session = sessionmaker(engine, autoflush=False)  
    session = Session()

    return [Zendesk_flight_cancelled_ticket, session]


def storingData(flightCancelledticket):
    Zendesk_flight_cancelled_ticket, session = connectionDB()



    print(len(flightCancelledticket))
    for i in flightCancelledticket:


    ##### flight num
    # ### flight date
    # ### Refund Res  
    ### Proccesing Remarks
        
        PNR_Number = None
        flightDate = None
        RefundReason = None
        flightNum = None
        paymentMethod = None
        reoundTripRefund = None
        
        ######### Retrive PNRs
        for fields in i['custom_fields']:
            # 360017856254 is the custom id field where the pax enter thair PNR (Zendesk system)
            if fields['id'] == 360017856254:
                PNR_Number = fields['value']
            if fields['id'] == 360017858514:
                flightDate = fields['value']
            if fields['id'] == 360006502319:
                RefundReason = fields['value']
            if fields['id'] == 360017858494:
                flightNum = fields['value']
            if fields['id'] == 360017880553:
                paymentMethod = fields['value']
            if fields['id'] == 360023086000:
                if fields['value'] == 'no_i_will_travel_on_return_flight_':
                    reoundTripRefund = 0
                if fields['value'] == 'yes_i_would_like_to_refund_the_return_flight_':
                    reoundTripRefund = 1
                

                
        ## if PNR not exsited ticket will be ignored till the agent update it plus if the PNR len over 6
        if PNR_Number != None and len(PNR_Number) <= 6:
            if i['id'] == 203489 or i['id'] == 203495:
                print(reoundTripRefund)
                print(i['id'])
            
            members = session.query(Zendesk_flight_cancelled_ticket).filter(and_(
                                Zendesk_flight_cancelled_ticket.ticket_id==i['id']
                                )).first()
            print(members)
            if members == None:
                print('adding')
                ticket = Zendesk_flight_cancelled_ticket(

                    ticket_id = i['id'],
                    PNR = PNR_Number,
                    created_at = i['created_at'],
                    description = i['description'],
                    priority = i['priority'],
                    requester_id = i['requester_id'],
                    status = i['status'],
                    subject = i['subject'],
                    tags = '::'.join(i['tags']),
                    updated_at = i['updated_at'],
                    url = i['url'],
                    flightDate = flightDate,
                    RefundReason = RefundReason,
                    flightNum = flightNum,
                    paymentMethod = paymentMethod,
                    record_status = 'open',
                    approval_header_id = null,
                    reoundTripStatus = reoundTripRefund
    
                    )
                session.add(ticket)
                updateAndCloseTickets(i['id'], 'pending', false, 'Ticket is under the refund system process')

    session.commit()
    session.close()
    print('commit')


def main():

    credentials = '', ''
    session = requests.Session()
    session.auth = credentials
    ##### only look for the refund form with the below tags and status of open or new
    params = {
        'query': 'type:ticket status:new status:open tags:refund tags:flight_cancellation tags:flight_cancelled tags:__dc.my_flight_was_cancelled__ tags:__dc.yes__',
        'sort_by': 'created_at',
        'sort_order': 'asc'  # from oldest to newest
    }


    url = 'https://flyadeal.zendesk.com/api/v2/search.json?' + urlencode(params)


    
    flightCancelledticket = []
    #### loop through the tickets pages via while method and append to 
    while url != None:
        response = session.get(url, verify=False)
        if response.status_code != 200:
            print('Status:', response.status_code, 'Problem with the request. Exiting.')
            exit()


        data = response.json()
        for result in data['results']:
            for tag in result['tags']:
                if 'flight' in tag:
                    ## 'canc' shot for cancelled or cancellation ..etc
                    if 'canc' in tag:
                        # to avoid dublication, check the appened list
                        if result not in flightCancelledticket:
                            flightCancelledticket.append(result)   
        
        url = data['next_page'] 

    storingData(flightCancelledticket)


main()