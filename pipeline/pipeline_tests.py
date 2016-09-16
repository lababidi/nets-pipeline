from pipeline.pipeline import HDA, EventArticle

import unittest


class TestPipeline(unittest.TestCase):
    def test_HDA(self):
        hda = HDA()
        text = '''BEIRUT, Lebanon — In rebel-held areas of Syria, Day 2 of a shaky cease-fire offered a rare chance to go
        outside. For some, that meant enjoying the simple pleasure of playing on a swing. For others, it meant protesting.

But no matter, it was a change from the routine of lives in a war zone, where days were often spent cowering in fear of
airstrikes.

Life During the Cease-fire
We are chronicling the experiences and observations of Syrians around the country during the partial truce.

DAY 1
Skepticism, but Also Hope
The cease-fire negotiated between Russia and the United States was supposed to allow for delivery of aid to the
divided city of Aleppo. Everything is needed: food, medicine, blankets. But United Nations officials said they had
not received a guarantee of safe passage, and truckloads of goods remained at the border with Turkey.

Diplomacy is driven by those in power — who live and work safely outside the war-torn country. But it is the people of
Syria who have the most at stake, and so we wanted to hear from them. For many, the pause in the fighting was a chance
to experience the routines of life that many take for granted: Taking a photograph of a friend, shopping for food.

“I think both sides, the opposition and regime, are tired of this war and want to have a break,” said Abu Yaman, a
father of four in Damascus.

Read more accounts below, and see more about the terms of the deal here.

Photo

An Eid festival in Dummar, a suburb of Damascus, on Wednesday. Credit Youssef Badawi/European Pressphoto Agency
Abu Yaman lives in Masakin Barzeh, in government-held Damascus, Syria’s capital. He spoke to a New York Times reporter
who is identifying him only by a nickname for his safety.


SYRIA
Damascus

Map Data
Terms of Use
We woke up late today and had a simple breakfast. We are going to my wife’s family to spend two days there. The Eid
vacation is long enough for visits and gives the children a good time before school starts in two weeks.

I am happy with the cease-fire, because all Syrians are tired of this absurd war. We wake up and go to sleep to the
number of killed, injured and displaced Syrians. We are tired of all of that.

I think both sides, the opposition and regime, are tired of this war and want to have a break. President Bashar said
that his regime will retake all the Syrian lands, but these are meaningless statements because he has no soldiers and
he is begging Russia, Iran and Hezbollah to send more fighters. The cease-fire is a good test for all sides, and it’s
good to wake up to the sounds of birds on my window — not the bombs and artilleries bombing Jobar and Douma.


CONFLICT IN SYRIA By MEGAN SPECIA 1:12
Syrian Nurse Explains Protests in Aleppo
Video
Syrian Nurse Explains Protests in Aleppo
Modar Shekho, a nurse in Aleppo, Syria, took part in a protest against international aid entering the city on Tuesday
evening. By MEGAN SPECIA on Publish Date September 14, 2016. Photo by Modar Shekho. Watch in Times Video »
 Embed
ShareTweet
Modar Shekho, 28, a nurse at al-Dakkak hospital in the rebel-held eastern section of Aleppo, took part in a protest
Tuesday night against the planned U.N. aid convoy. Syrians there said that any aid delivery that required permission
from the government would legitimize the siege. They called instead for free movement of goods and people — but Russia
and the Syrian government say that is a security risk.

Mr. Shekho filmed the protest with his smartphone, showing a few dozen people, some carrying signs with a dark red X
over the letters “U.N.” and flags of the Free Syrian Army and the rebranded Nusra Front.


TURKEY
Aleppo
SYRIA

Map Data
Terms of Use
People here are annoyed by the bad situation in Aleppo. All of the basic needs are highly taxed, and because of the
siege, there is no oil. There are no basic things necessary for life.

People here need to ambulance some people to Turkey to get some care. People need a lot of other things, not this aid.
People need their freedom. People said in this demonstration that they don’t need this aid, that they wanted oil,
medical supplies. And they wanted the road out from Aleppo.


CONFLICT IN SYRIA By YARA BISHARA 1:13
In a Besieged Town, No Food and No Escape
Video
In a Besieged Town, No Food and No Escape
Hala Abdulwahab, 24, is a teacher in Madaya, Syria, near the Lebanese border. Civilians in the town struggle to get
food on a daily basis. By YARA BISHARA on Publish Date September 14, 2016. Photo by Omar Sanadiki/Reuters. Watch in
Times Video »
 Embed
ShareTweet
Hala Abdulwahab, 24, a teacher in the besieged town of Madaya near the Lebanese border, spent the holiday taking
advantage of the cease-fire to tour the town.

Madaya, Syria

TURKEY
SYRIA
Madaya
JORDAN

Map Data
Terms of Use
I spent my day out, taking photos of children celebrating Eid. It was Eid, and it wasn’t. I visited a friend of mine,
a widow, her husband is a martyr. She has five children. I took photos of her washing the dishes.

I also visited my other friend, another schoolteacher, going through a bad depression. Her husband was evacuated from
Zabadani a few months ago, and she was hoping to join him shortly. She’s missing him a lot. Unfortunately, he was
imprisoned by the Free Syrian Army after they charged him with looting.

I went to the souk for some shopping. It was really crowded, but the prices were superhigh. I bought salt, lemon salt,
peppers, Maggi soup cubes, half a liter of oil and 10 cigarettes. I paid $50.

So will the cease-fire continue until tomorrow night? Let me take the chance and go to the other side of the town and
take some photos before it ends.


CONFLICT IN SYRIA By MEGAN SPECIA and YARA BISHARA 00:40
The Eyes of a Syrian Activist
Video
The Eyes of a Syrian Activist
Idlib Province in Syria was hit by deadly airstrikes ahead of the Eid al-Adha holiday, which began on Monday, the same
day as a cease-fire. An area resident, Muhammed Najdat Kaddour, filmed the scene. By MEGAN SPECIA and YARA BISHARA on
Publish Date September 12, 2016. . Watch in Times Video »
 Embed
ShareTweet
Muhammed Najdat Kaddour, 31, a citizen journalist from the rebel-held province of Idlib, took a rare day off from
documenting the war, and spoke via internet chat.


Binnish
SYRIA
TURKEY

Map Data
Terms of Use
Honestly, I didn’t cover any news today. For the first time, I spent the day swinging with my niece. For the first time,
I didn’t put on the walkie-talkie to get updates about the movements of the warplanes. I toured around Taftanaz,
Binnish, Idlib city. I saw my friends, I listened to music. It’s the first time I turned the music on in the car.
Usually, I turn the radio off to listen to the sounds of the warplanes in the sky.'''
        self.maxDiff = None
        article = EventArticle(0, content=text)
        hda.process(article)
        categories = {h['name'] for h in article.hda}
        true_categories = {'Government: Security and Public Services: Police',
                           'Economy: Commerce: Arts and Entertainment',
                           'People and Society: Political',
                           'Infrastructure: Transportation: Ports: Air',
                           'Economy: Commerce: Personal Care Services',
                           'Economy: Commerce: Computer and Electronics',
                           'Economy: Commerce: Food and Dining',
                           'People and Society: Demographics',
                           'Economy: Commerce: Shopping',
                           'Infrastructure: Communications and Media',
                           'People and Society: Ethnicity',
                           'Infrastructure: Health and Medicine',
                           'Economy: Commerce: Travel and Transportation',
                           'Infrastructure: Education',
                           'Infrastructure: Utilities: Power',
                           'Government: Security and Public Services',
                           'Economy: Commerce: Industry and Agriculture',
                           'Government: Security and Public Services: Emergency',
                           'Infrastructure: Transportation: Road',
                           'Economy: Commerce: Legal and Financial'}
        self.assertEqual(true_categories, categories)
        print(article.hda)
