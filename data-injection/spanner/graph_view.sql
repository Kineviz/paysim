CREATE OR REPLACE PROPERTY GRAPH graph_view
NODE TABLES (
    Client
        KEY (id)
        LABEL Client
        PROPERTIES (id, name, isfraud),
    Merchant
        KEY (id)
        LABEL Merchant
        PROPERTIES (id, name, highrisk),
    Bank
        KEY (id)
        LABEL Bank
        PROPERTIES (id, name),
    Transaction
        KEY (id)
        LABEL Transaction
        PROPERTIES (id, amount, timestamp, action, globalstep, isfraud, isflaggedfraud, typedest, typeorig),
    Email
        KEY (id)
        LABEL Email
        PROPERTIES (id, name),
    PhoneNumber
        KEY (id)
        LABEL PhoneNumber
        PROPERTIES (id, name),
    SSN
        KEY (id)
        LABEL SSN
        PROPERTIES (id, name)
)
EDGE TABLES(
    Client_Perform_Transaction
        KEY (client_id, transaction_id, timestamp)
        SOURCE KEY (client_id) REFERENCES Client (id)
        DESTINATION KEY (transaction_id) REFERENCES Transaction (id)
        LABEL PERFORMS
        PROPERTIES (timestamp, client_id, transaction_id),
    Transaction_To_Client
        KEY (transaction_id, client_id, timestamp)
        SOURCE KEY (transaction_id) REFERENCES Transaction (id)
        DESTINATION KEY (client_id) REFERENCES Client (id)
        LABEL TO_CLIENT
        PROPERTIES (timestamp, transaction_id, client_id),
    Transaction_To_Merchant
        KEY (transaction_id, merchant_id, timestamp)
        SOURCE KEY (transaction_id) REFERENCES Transaction (id)
        DESTINATION KEY (merchant_id) REFERENCES Merchant (id)
        LABEL TO_MERCHANT
        PROPERTIES (timestamp, transaction_id, merchant_id),
    Transaction_To_Bank
        KEY (transaction_id, bank_id, timestamp)
        SOURCE KEY (transaction_id) REFERENCES Transaction (id)
        DESTINATION KEY (bank_id) REFERENCES Bank (id)
        LABEL TO_BANK
        PROPERTIES (timestamp, transaction_id, bank_id),
    Has_Email
        KEY (client_id, email_id)
        SOURCE KEY (client_id) REFERENCES Client (id)
        DESTINATION KEY (email_id) REFERENCES Email (id)
        LABEL HAS_EMAIL
        PROPERTIES (client_id, email_id),
    Has_PhoneNumber
        KEY (client_id, phonenumber_id)
        SOURCE KEY (client_id) REFERENCES Client (id)
        DESTINATION KEY (phonenumber_id) REFERENCES PhoneNumber (id)
        LABEL HAS_PHONE
        PROPERTIES (client_id, phonenumber_id),
    Has_SSN
        KEY (client_id, ssn_id)
        SOURCE KEY (client_id) REFERENCES Client (id)
        DESTINATION KEY (ssn_id) REFERENCES SSN (id)
        LABEL HAS_SSN
        PROPERTIES (client_id, ssn_id)
)