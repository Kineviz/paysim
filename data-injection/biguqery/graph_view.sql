
    CREATE OR REPLACE PROPERTY GRAPH paysim_graph.graph_view
    NODE TABLES (
        paysim_graph.Client
            KEY (id)
            LABEL Client
            PROPERTIES (id, name, isfraud),
        paysim_graph.Merchant
            KEY (id)
            LABEL Merchant
            PROPERTIES (id, name, highrisk),
        paysim_graph.Bank
            KEY (id)
            LABEL Bank
            PROPERTIES (id, name),
        paysim_graph.Transaction
            KEY (id)
            LABEL Transaction
            PROPERTIES (id, amount, timestamp, action, globalstep, isfraud, isflaggedfraud,
                       typedest, typeorig),
        paysim_graph.Email
            KEY (id)
            LABEL Email
            PROPERTIES (id, name),
        paysim_graph.PhoneNumber
            KEY (id)
            LABEL PhoneNumber
            PROPERTIES (id, name),
        paysim_graph.SSN
            KEY (id)
            LABEL SSN
            PROPERTIES (id, name)
    )
    EDGE TABLES(
        paysim_graph.Client_Perform_Transaction
            KEY (client_id, transaction_id, timestamp)
            SOURCE KEY (client_id) REFERENCES Client (id)
            DESTINATION KEY (transaction_id) REFERENCES Transaction (id)
            LABEL PERFORMS
            PROPERTIES (timestamp, client_id, transaction_id),
        paysim_graph.Transaction_To_Client
            KEY (transaction_id, client_id, timestamp)
            SOURCE KEY (transaction_id) REFERENCES Transaction (id)
            DESTINATION KEY (client_id) REFERENCES Client (id)
            LABEL TO_CLIENT
            PROPERTIES (timestamp, transaction_id, client_id),
        paysim_graph.Transaction_To_Merchant
            KEY (transaction_id, merchant_id, timestamp)
            SOURCE KEY (transaction_id) REFERENCES Transaction (id)
            DESTINATION KEY (merchant_id) REFERENCES Merchant (id)
            LABEL TO_MERCHANT
            PROPERTIES (timestamp, transaction_id, merchant_id),
        paysim_graph.Transaction_To_Bank
            KEY (transaction_id, bank_id, timestamp)
            SOURCE KEY (transaction_id) REFERENCES Transaction (id)
            DESTINATION KEY (bank_id) REFERENCES Bank (id)
            LABEL TO_BANK
            PROPERTIES (timestamp, transaction_id, bank_id),
        paysim_graph.Has_Email
            KEY (client_id, email_id)
            SOURCE KEY (client_id) REFERENCES Client (id)
            DESTINATION KEY (email_id) REFERENCES Email (id)
            LABEL HAS_EMAIL
            PROPERTIES (client_id, email_id),
        paysim_graph.Has_PhoneNumber
            KEY (client_id, phonenumber_id)
            SOURCE KEY (client_id) REFERENCES Client (id)
            DESTINATION KEY (phonenumber_id) REFERENCES PhoneNumber (id)
            LABEL HAS_PHONE
            PROPERTIES (client_id, phonenumber_id),
        paysim_graph.Has_SSN
            KEY (client_id, ssn_id)
            SOURCE KEY (client_id) REFERENCES Client (id)
            DESTINATION KEY (ssn_id) REFERENCES SSN (id)
            LABEL HAS_SSN
            PROPERTIES (client_id, ssn_id)
    );
    